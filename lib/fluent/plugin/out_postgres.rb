class Fluent::PostgresOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('postgres', self)

  include Fluent::SetTimeKeyMixin
  include Fluent::SetTagKeyMixin

  config_param :host, :string
  config_param :port, :integer, :default => nil
  config_param :database, :string
  config_param :username, :string
  config_param :password, :string, :default => ''

  config_param :key_names, :string, :default => nil # nil allowed for json format
  config_param :sql, :string, :default => nil
  config_param :table, :string, :default => nil
  config_param :columns, :string, :default => nil

  config_param :format, :string, :default => "raw" # or json
  config_param :force_encoding, :string, :default => nil

  attr_accessor :handler

  def initialize
    super
    require 'pg'
    require 'json'
  end

  # We don't currently support mysql's analogous json format
  def configure(conf)
    super

    if @format == 'json'
      @format_proc = Proc.new{|tag, time, record| record.to_json}
    else
      @key_names = @key_names.split(',')
      @format_proc = Proc.new{|tag, time, record| @key_names.map{|k| record[k]}}
    end

    if @columns.nil? and @sql.nil?
      raise Fluent::ConfigError, "columns or sql MUST be specified, but missing"
    end
    if @columns and @sql.nil?
      keys = columns.split(",").map{|x| x.strip}
      key_count = 1..keys.size()
      place_holders = key_count.to_a.map {|x| '$'<<x.to_s}
      @sql = "INSERT INTO " << @table << " (" << keys.join(",") << ") VALUES (" << place_holders.join(",") << ")"
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    record = set_encoding(record) if @force_encoding
    [tag, time, @format_proc.call(tag, time, record)].to_msgpack
  end

  def client
    PG::Connection.new({
      :host => @host, :port => @port,
      :user => @username, :password => @password,
      :dbname => @database
    })
  end

  def write(chunk)
    begin
      handler = self.client
      handler.prepare("write", @sql)
      chunk.msgpack_each { |tag, time, data|
        data = set_encoding(data) if @force_encoding
        data = JSON.parse(data).values if @format == 'json'
        handler.exec_prepared("write", data)
      }
      $log.info "completed insert data into postgres. TABLE: #{@table}"
    rescue PG::Error => e
      $log.error "failed to insert data into postgres. TABLE: #{@table}", :error=>e.to_s
      raise e
    ensure
      handler.close rescue nil if handler
    end
  end

  def set_encoding(record)
    record.each_pair { |k, v|
      if v.is_a?(String)
        v.force_encoding(@force_encoding)
      end
    }
  end

end
