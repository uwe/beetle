require "rubygems"
require File.expand_path("../lib/beetle", File.dirname(__FILE__))

# set Beetle log level to info, less noisy than debug
Beetle.config.logger.level = Logger::INFO
Beetle.config.servers = "localhost:5672, localhost:5673"

# setup client
client = Beetle::SimpleClient.new

#############################
# Example: A simple message #
#############################
# register a message
client.register_message(:a_message)
# and specify a handler that is interested in this message
client.register_handler("modest handler", :a_message) {|message| puts "modest handler got message: #{message.data}"}

######################################################
# Example: Messages with params and multiple messages
# passing options to messages still works
client.register_message(:another_message, :redundant => true)
# now let's register to multiple messages
client.register_message(:unimportant_message)
client.register_handler("greedy handler", :a_message, :another_message, :unimportant_message) {|message| puts "greedy handler got message: #{message.data}"}

###########################
# Example: Message groups #
###########################
# what if we have a whole bunch of messages of the same family which we wanna listen to?
client.register_message(:user_created, :group => :user)
client.register_message(:user_deleted, :group => :user)
client.register_message(:user_updated, :group => :user)
# this handler will receive all user created/deleted/updated messages
client.register_handler("user handler", :groups => :user) {|message| puts "user handler got message: #{message.data}"}

# that also works for multiple groups
client.register_message(:admin_granted, :group => :system)
client.register_message(:admin_revoked, :group => :system)
# this handler will receive all messages from the system and the user group, as well as the earlier defined a_message
client.register_handler("system handler", :a_message, :groups => [:user, :system]) {|message| puts "system handler got message: #{message.data}"}

###########################
# Example: Handler classes #
###########################
# if you wanna use a class instead of a proc, make sure it implements the call method :)
class ClassyHandler; def self.call(message); puts "the classy handler got message: #{message.data}"; end; end
client.register_handler(ClassyHandler, :a_message)


# That's not part of the setup, just some cleanup
client.deduplication_store.flushdb
client.purge("greedy handler")
client.purge("modest handler")
client.purge("user handler")
client.purge("system handler")

# publish our message
client.publish(:a_message, 'some random message')
client.publish(:another_message, 'another message')
client.publish(:unimportant_message, 'peter paul and marry are sitting in the kitchen')
client.publish(:user_deleted, "a user got deleted")
client.publish(:admin_granted, "a user got admin rights granted")

# start listening
# this starts the event machine event loop using EM.run
# the block passed to listen will be yielded as the last step of the setup process
client.listen do
  EM.add_timer(0.1) { client.stop_listening }
end

