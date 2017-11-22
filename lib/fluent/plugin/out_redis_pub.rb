#
# Copyright 2017- John-Lin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'redis'
require 'json'
require 'msgpack'
require "fluent/plugin/output"

module Fluent
  module Plugin
    class RedisPubOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_output("redis_pub", self)

      helpers :compat_parameters, :inject

      DEFAULT_BUFFER_TYPE = "memory"

      attr_reader :redis

      config_param :host, :string, default: 'localhost'
      config_param :port, :integer, default: 6379
      config_param :db_number, :integer, default: 0
      config_param :password, :string, default: nil, secret: true
      config_param :channel, :string, default: "${tag}"
      config_param :format, :string, default: 'json'

      config_section :buffer do
        config_set_default :@type, DEFAULT_BUFFER_TYPE
        config_set_default :chunk_keys, ['tag', 'time']
        config_set_default :timekey, 60
      end

      def configure(conf)
        compat_parameters_convert(conf, :buffer, :inject)
        super

        raise Fluent::ConfigError, "'tag' in chunk_keys is required." if not @chunk_key_tag
        raise Fluent::ConfigError, "'time' in chunk_keys is required." if not @chunk_key_time
        @unpacker = Fluent::Engine.msgpack_factory.unpacker
      end

      def start
        super

        options = {
          host: @host,
          port: @port,
          thread_safe: true,
          db: @db_number
        }
        options[:password] = @password if @password

        @redis = Redis.new(options)
      end

      def shutdown
        @redis.quit
        super
      end

      def format(tag, time, record)
        record = inject_values_to_record(tag, time, record)
        log = {
          :tag => tag,
          :time => time,
          :record => record
        }
        log.to_msgpack
      end

      def formatted_to_msgpack_binary
        true
      end

      def write(chunk)
        @redis.pipelined do
          chunk.msgpack_each do |record|
            @redis.publish @channel, record.to_json
          end
        end
      end

    end
  end
end
