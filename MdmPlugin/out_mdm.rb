# This is MDM output plugin: data will be sent to MDM through IFX API.
module Fluent
    class OutputMDM < BufferedOutput
        Plugin.register_output('mdm', self)

        def initialize
            super
            require_relative 'Libifxext'
            require 'time'

            # A hash table to save MDM metric objects.
            # Key: string combining MDM namespace, metrics, dimension names.
            # Value: MDM metrics objects defined in Libifxext module.
            @@mdm2dtable = {}
        end

        desc 'MDM account'
        config_param :mdm_account, :string

        def configure(conf)
            super

            # Add RTCPAL level loggings for these log levels
            loglevels_for_rtcpal = ["debug", "trace"]
            if loglevels_for_rtcpal.include? log_level
                ENV["RTCPAL_LOG_LEVEL"] = "2"
                ENV["RTCPAL_LOG_IDENT"] = "OutputMDM"
            end
        end

        def start
            super
            Libifxext::MdmStartup()
        end

        def shutdown
            super
            Libifxext::MdmCleanup()
        end

        # This method is called when an event reaches to Fluentd.
        # Convert the event to a raw string.
        def format(tag, time, record)
            [tag, record].to_msgpack
        end

        # This method is called every flush interval.
        # 'chunk' is a buffer chunk that includes multiple formatted events.
        # Return true if success, false if any error.
        def write(chunk)
            isSuccess = true
            begin
                chunk.msgpack_each {|(tag, record)|
                    if OutputMDMHelper.is_oms_perf_record(record)
                        if not handle_oms_perf_record(record)
                            isSuccess = false
                        end
                    else
                        @log.error "Error: unsupported record type for tag #{tag}. Ignore data."
                        isSuccess = false
                    end
                }
            rescue Exception => ex
                @log.error "Error: unexpected exception #{ex.message}."
                isSuccess = false
            end
            return isSuccess
        end

        # Send OMS record to MDM agent.
        # Return true if success, false if any error.
        # NOTE:
        #  - Each OmsAgent record has an array called DataItems.
        #  - Each DataItem has an array called Collections.
        #  - Each Collection is {CounterName, Value}.
        def handle_oms_perf_record(record)
            isSuccess = true
            dataitems = record["DataItems"]
            dataitems.each { |item|
                timeticks = OutputMDMHelper.GetTicksFromWinTimeStr(item["Timestamp"])
                hostname = item["Host"]
                objname = item["ObjectName"]
                instname = item["InstanceName"]
                collections = item["Collections"]

                instanceDimVal = instname

                collections.each { |collection|
                    if not handle_2d_metrix(timeticks, objname, collection["CounterName"],
                        "Region", hostname, "InstanceName", instanceDimVal, collection["Value"].to_i())
                        isSuccess = false
                    end
                }
            }
            return isSuccess
        end

        # Send a 2D metrics to MDM agent.
        # Return true if success, false if any error.
        def handle_2d_metrix(timeticks, mdm_namespace, metrics_name,
                             dim1name, dim1val, dim2name, dim2val, metrics_value)
            tracestr = "namespace=#{mdm_namespace}, metrics=#{metrics_name}, d1=#{dim1name}, d1val=#{dim1val}; d2=#{dim2name}, d2val=#{dim2val}, metricsval=#{metrics_value}"
            @log.trace "Sending to MDM: #{tracestr}"

            mdm2dkey = mdm_namespace + "::" + metrics_name + "::" + dim1name + "::" + dim2name
            m = @@mdm2dtable[mdm2dkey]
            if not m
                m = Libifxext::Mdm2D.new(mdm_account, mdm_namespace, metrics_name, dim1name, dim2name)
                @@mdm2dtable[mdm2dkey] = m
            end

            isSuccess = m.LogValueAtTime(timeticks, metrics_value, dim1val, dim2val)
            if not isSuccess
                @log.error "Sending to MDM agent failed: #{tracestr}"
            end
            return isSuccess
        end

    end # class OutputMDM
end  # module Fluent

class OutputMDMHelper
    # Epoch time used by windows SystemTime
    @@win_epoch_ticks= Time.utc(1601,1,1).to_i() * 10000000

    # Check whether this is an OMS perf record by using certain record keys.
    def self.is_oms_perf_record(record)
        datatype = record["DataType"]
        if datatype && (datatype == "LINUX_PERF_BLOB") && (record.key? "DataItems")
            return true
        else
            return false
        end
    end

    # Get number of ticks for a given UTC time since 1601/01/01 00:00:00.
    # Example input: "2016-06-28T21:58:24.677Z", returns: 131116247046770000
    def self.GetTicksFromWinTimeStr(timestampStr)
        t1 = Time.parse(timestampStr)
        return (t1.to_i() * 10000000 + t1.nsec/100 - @@win_epoch_ticks)
    end

end # OutputMDMHelper
