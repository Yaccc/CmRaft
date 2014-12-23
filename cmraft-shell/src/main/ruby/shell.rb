include Java
require 'pathname'

source = java.lang.System.getProperty('cmraft.ruby.source')
$LOAD_PATH.unshift Pathname.new(source)

module Shell
  @@commands = {}
  @@connection = Java::com.chicm.cmraft.ConnectionManager.getConnection()	
  @@exitCode = "exit"
  def self.commands
    @@commands
  end
  
  def self.connection
    @@connection
  end
  
  @@command_groups = {}
  def self.command_groups
    @@command_groups
  end

  def self.load_command(name, group, aliases=[])
    return if commands[name]

    # Register command in the group
    raise ArgumentError, "Unknown group: #{group}" unless command_groups[group]
    command_groups[group][:commands] << name

    # Load command
    begin
      require "commands/#{name}"
      klass_name = name.to_s.gsub(/(?:^|_)(.)/) { $1.upcase } # camelize
      commands[name] = eval("Commands::#{klass_name}")
      aliases.each do |an_alias|
        commands[an_alias] = commands[name]
      end
    rescue => e
      raise "Can't load cmraft shell command: #{name}. Error: #{e}\n#{e.backtrace.join("\n")}"
    end
  end

  def self.load_command_group(group, opts)
    raise ArgumentError, "No :commands for group #{group}" unless opts[:commands]

    command_groups[group] = {
      :commands => [],
      :command_names => opts[:commands],
      :full_name => opts[:full_name] || group,
      :comment => opts[:comment]
    }

    all_aliases = opts[:aliases] || {}

    opts[:commands].each do |command|
      aliases = all_aliases[command] || []
      load_command(command, group, aliases)
    end
  end

  def self.getCommand()
	print "cmraft>:"
	lastCommand = gets.chomp
            
    lastCommand = '' if (not lastCommand.to_s.empty? and lastCommand[0] == '#')
	#puts "lastcommand" << lastCommand
    return lastCommand
  end
  
  def self.processExitCommand(cmd, *params)
  	return 0 if(cmd != "exit" and cmd != "quit") 		
  	return @@exitCode
  end
   
  def self.processCommand(commandLine)
    return "" if(commandLine.to_s.empty?)
    cmd, *params = commandLine.split   
    return @@exitCode if(@@exitCode == processExitCommand(cmd, *params))
	
	if(commands[cmd])
	  cmdobj = commands[cmd].new()
	  cmdobj.command(*params)
	else
	  puts "command not recognized: " << cmd
	end
	return "success"
  end
   
# Load commands base class
#require 'shell/commands'

Shell.load_command_group(
  'kvs',
  :full_name => 'Key value store commands',
  :commands => %w[
    set
    list
    get
    delete
  ]
)

if(!@@connection) then
	puts "Could not connect to Raft servers, exiting..."
	Java::java.lang.System.exit(0)
end

loop do
begin                 
    command = Shell.getCommand()
    result = Shell.processCommand(command)
    break if result == @@exitCode
    rescue Exception => e
      raise "Error: #{e}\n#{e.backtrace.join("\n")}"
    end
end
  
Java::java.lang.System.exit(0)

end