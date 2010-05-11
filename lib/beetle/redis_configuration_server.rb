require 'timeout'
module Beetle
  class RedisConfigurationServer < Beetle::Handler

    @@active_master         = nil
    @@client                = Beetle::Client.new
    @@alive_servers         = {}
    @@reconfigured_answers  = {}
    cattr_accessor :client
    cattr_accessor :active_master
    cattr_accessor :alive_servers
    cattr_accessor :reconfigured_answers

    class << self
      def active_master_reachable?
        if active_master
          return true if reachable?(active_master)
          client.config.redis_configuration_master_retries.times do
            sleep client.config.redis_configuration_master_retry_timeout.to_i
            return true if reachable?(active_master)
          end
        end
        false
      end

      def find_active_master(force_change = false)
        if !force_change && active_master
          return if reachable?(active_master)
          client.config.redis_configuration_master_retries.times do
            sleep client.config.redis_configuration_master_retry_timeout.to_i
            return if reachable?(active_master)
          end
        end
        available_redis_server = (client.deduplication_store.redis_instances - [active_master]).sort_by {rand} # randomize redis stores to not return the same one on missed promises
        available_redis_server.each do |redis|
          reconfigure(redis) and break if reachable?(redis)
        end
      end

      def give_master(payload)
        # stores our list of servers and their ping times
        alive_servers[payload['server_name']] = Time.now # unless vote_in_progess
        active_master || 'undefined'
      end

      def reconfigure(new_master)
        client.publish(:reconfigure, {:host => new_master.host, :port => new_master.port}.to_json)
        setup_reconfigured_check_timer(new_master)
      end

      def reconfigured(payload)
        reconfigured_answers[payload['sender_name']] = payload['acked_server']
      end

      def going_offline(payload)
        alive_servers[payload['sender_name']] = nil
      end

      def server_alive?(server)
        alive_servers[server] && (alive_servers[server] > Time.now - 10.seconds)
      end

      def switch_master(new_master)
        new_master.slaveof('NO ONE')
        active_master = new_master
      end

      def reset
        self.alive_servers = {}
        self.active_master = {}
      end

      private
      def clear_active_master
        self.active_master = nil
      end

      def setup_reconfigured_check_timer(new_master)
        EM.add_timer(client.config.redis_configuration_reconfiguration_timeout.to_i) do 
          check_reconfigured_answers(new_master)
        end
      end

      def check_reconfigured_answers(new_master)
        if all_alive_servers_reconfigured?(new_master)
          switch_master(new_master)
        else
          setup_reconfigured_check_timer(new_master)
        end
      end

      def all_alive_servers_reconfigured?(new_master)
        reconfigured_answers.all? {|k,v| v == new_master.server}
      end

      def reachable?(redis)
        begin
          Timeout::timeout(5) {
            !!redis.info
          }
        rescue Timeout::Error => e
          false
        end
      end
    end

    def process
      hash = ActiveSupport::JSON.decode(message.data)
      self.class.__send__(hash.delete("op").to_sym, hash)
    end

  end
end