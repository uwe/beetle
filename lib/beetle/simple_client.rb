require 'ruby-debug'
Debugger.start
module Beetle
  # raised when a handler is tried to access which doesn't exist
  class UnknownHandlerError < Error; end

  class SimpleClient < Client
    private :register_binding, :register_queue, :register_exchange

    def register_message(message_name, options={})
      options.assert_valid_keys(:group, :redundant)
      group = options.delete(:group)
      options[:key] = "#{group}.#{message_name}" if group
      super
    end

    # FIXME: move into some small and nice methods
    def register_handler(handler, *messages_to_listen, &block)
      raise ArgumentError.new("Either a handler class or a block (in case of a named handler) must be given") if handler.is_a?(String) && !block_given?
      queue = queue_name_from_handler(handler)
      handler_opts = messages_to_listen.last.is_a?(Hash) ? messages_to_listen.pop : {}
      queue_opts = handler_opts.slice!(:errback, :failback, :group)

      begin
        register_queue queue, queue_opts
      rescue ConfigurationError
        raise ConfigurationError.new("Handler names must be unique")
      end

      messages_to_listen.each do |message_name|
        message = messages[message_name.to_s]
        register_binding queue, :key => message[:key], :exchange => message[:exchange]
      end

      if group = handler_opts.delete(:group)
        raise ConfigurationError.new("no messages for group #{group} specified") unless messages.any? {|_, opts| opts[:key] =~ /^#{group}\./}
        register_binding queue, :key => "#{group}.#", :exchange => messages[:exchange]
      end

      if handler.is_a?(Class)
        super(queue, handler, handler_opts)
      else
        super(queue, handler_opts, &block)
      end
    end
    
    def handler(handler)
      handler_name = queue_name_from_handler(handler)
      if queues.has_key? handler_name
        SimpleHandler.new(handler_name, self)
      else
        raise UnknownHandlerError.new
      end
    end

    def purge(handler)
      super(queue_name_from_handler(handler))
    end

    private
    def queue_name_from_handler(handler)
      handler.is_a?(Class) ? handler.name.underscore.gsub('/', '.') : handler.gsub(' ', '_').underscore
    end
    
    class SimpleHandler # nodoc
      def initialize(handler_name, client)
        @client = client
        @name = handler_name
      end

      def listens_to?(message)
        message = @client.messages[message.to_s]
        @client.bindings[@name].any? do |binding|
          same_exchange = binding[:exchange] == message[:exchange]
          key_matches = if binding[:key] =~ /(.+)\.\#$/
                          group = $1
                          !!message[:key] =~ /^#{group}\./
                        else
                          binding[:key] == message[:key]
                        end
          key_matches && same_exchange
        end
      end
    end
  end
end