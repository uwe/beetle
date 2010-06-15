require File.expand_path(File.dirname(__FILE__) + '/../test_helper')

module Beetle
  class SimpleClientRegisterMessage < Test::Unit::TestCase
    def setup
      @client = SimpleClient.new
    end

    test "should be able to register messages" do
      @client.register_message(:some_message)
      assert @client.messages.has_key?("some_message")
    end

    test "should use the message name for the routing key" do
      @client.register_message(:some_message)
      assert_equal "some_message", @client.messages["some_message"][:key]
    end

    test "should allow groups of messages by using a nemespace in the routing key" do
      @client.register_message(:some_message, :group => :some_group)
      assert_equal "some_group.some_message", @client.messages["some_message"][:key]
    end

    test "should only allow a subset of the regular clients message params" do
      @client.register_message(:some_message1, :redundant => true)

      assert_raises ArgumentError do
        @client.register_message(:some_message2, :exchange => :foobar)
      end
      assert_raises ArgumentError do
        @client.register_message(:some_message3, :key => "some_key")
      end

    end
  end

  class SimpleClientRegisterHandler < Test::Unit::TestCase
    def setup
      @client = SimpleClient.new
      @client.register_message(:my_message)
    end

    class ::MyHandler; def call; end; end
    module ::MyNamespace; class MyHandler; end; end;

    test "should accept a Handler class" do
      @client.register_handler(MyHandler, :my_message)
      assert_equal MyHandler, @client.send(:subscriber).handlers["my_handler"][1]
    end

    test "should accept a string" do
      my_handler = lambda {}
      @client.register_handler("my handler", :my_message, &my_handler)
      assert_equal my_handler, @client.send(:subscriber).handlers["my_handler"][1]
    end

    test "should require a block if a string is provided as a handler" do
      assert_raises ArgumentError do
        @client.register_handler("my handler", :my_message)
      end
    end

    test "should create a queue with the name of the hander" do
      @client.register_handler("my second handler", :my_message) {}
      @client.register_handler(MyHandler, :my_message)
      @client.register_handler(MyNamespace::MyHandler, :my_message)

      assert @client.queues.has_key? "my_handler"
      assert @client.queues.has_key? "my_second_handler"
      assert @client.queues.has_key? "my_namespace.my_handler"
    end

    test "should require the handler name argument to be unique" do
      @client.register_handler(MyHandler, :my_message)
      assert_raises ConfigurationError do
        @client.register_handler(MyHandler, :my_message)
      end
    end

    test "should bind the handler to every message specified" do
      @client.register_message(:my_other_message)
      @client.register_handler(MyHandler, :my_message, :my_other_message)
      assert @client.handler(MyHandler).listens_to?(:my_message)
      assert @client.handler(MyHandler).listens_to?(:my_other_message)
    end

    test "should allow to set a errback and a failback callback" do
      errback = lambda {}
      failback = lambda {}
      @client.register_handler(MyHandler, :my_message, :errback => errback, :failback => failback)
      assert_equal errback, @client.send(:subscriber).handlers["my_handler"][0][:errback]
      assert_equal failback, @client.send(:subscriber).handlers["my_handler"][0][:failback]
    end
    
    test "should allow to listen to groups of messages" do
      @client.register_message(:message_1_of_group_1, :group => :testgroup_1)
      @client.register_message(:message_2_of_group_1, :group => :testgroup_1)
      @client.register_message(:message_1_of_group_2, :group => :testgroup_2)
      @client.register_handler("group handler",           :groups => :testgroup_1) {}
      @client.register_handler("multiple groups handler", :groups => [:testgroup_1, :testgroup_2]) {}
      @client.register_handler("hybrid handler",          :my_message, :groups => :testgroup_1) {}

      assert @client.handler("group handler").listens_to?(:message_1_of_group_1)
      assert @client.handler("group handler").listens_to?(:message_2_of_group_1)

      assert @client.handler("multiple groups handler").listens_to?(:message_1_of_group_1)
      assert @client.handler("multiple groups handler").listens_to?(:message_2_of_group_1)
      assert @client.handler("multiple groups handler").listens_to?(:message_1_of_group_2)

      assert @client.handler("hybrid handler").listens_to?(:message_1_of_group_1)
      assert @client.handler("hybrid handler").listens_to?(:message_2_of_group_1)
      assert @client.handler("hybrid handler").listens_to?(:my_message)
    end

    test "should not allow to bind to messages that aren't defined" do
    end
    
    test "should not allow messages with the same name as handlers" do      
    end
  end
end