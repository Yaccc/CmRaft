module Shell
  @@commands = {}
  def self.commands
    @@commands
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
      require "./commands/#{name}.rb"
      klass_name = name.to_s.gsub(/(?:^|_)(.)/) { $1.upcase } # camelize
      commands[name] = eval("Commands::#{klass_name}")
      aliases.each do |an_alias|
        commands[an_alias] = commands[name]
      end
    rescue => e
      raise "Can't load hbase shell command: #{name}. Error: #{e}\n#{e.backtrace.join("\n")}"
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

# Load commands base class
#require 'shell/commands'

Shell.load_command_group(
  'kvs',
  :full_name => 'Key value store commands',
  :commands => %w[
    set
    list
  ]
)

end