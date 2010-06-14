module Beetle
  # raised when two redis master servers are found
  class UnknownHandlerError < Error; end

  class SimpleClient < Client
    private :register_binding, :register_queue

    def register_message(message_name, options={})
      options.assert_valid_keys(:group)
      group = options.delete(:group)
      options[:key] = "#{group}.#{message_name}" if group
      super
    end
    
    def register_handler(handler, *messages_to_listen, &block)
      raise ArgumentError.new("Either a handler class or a block (in case of a named handler) must be given") if handler.is_a?(String) && !block_given?
      queue = queue_name_from_handler(handler)
      queue_opts = messages_to_listen.last.is_a?(Hash) ? messages_to_listen.pop : {}

      begin
        register_queue queue, queue_opts
      rescue ConfigurationError
        raise ConfigurationError.new("Handler names must be unique")
      end

      messages_to_listen.each do |message_name|
        message = messages[message_name.to_s]
        register_binding queue, :key => message[:key], :exchange => message[:exchange]
      end

      if handler.is_a?(Class)
        super(queue, handler)
      else
        super(queue, {}, &block)
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
      def bound_to?(message)
        message = @client.messages[message.to_s]
        @client.bindings[@name].any? {|binding| binding[:key] == message[:key] && binding[:exchange] == message[:exchange]}
      end
    end
  end
end