module Rack
  module RedisAnalytics

    class DataSet
      attr_writer :dimension
      attr_reader :to_date_time
      attr_writer :unit
      attr_reader :aggregate
      attr_writer :units
      attr_reader :params



      FORMAT_SPECIFIER = [['%Y', 365], ['%m', 30], ['%d', 24], ['%H', 60], ['%M', 60]]

      GRANULARITY = ['yearly', 'monthly', 'dayly', 'hourly', 'minutely']

      def dimension
        @dimension || 'global'
      end

      def to_date_time=(v)
        @to_date_time = Date.parse(v).to_time rescue Time.now
      end

      def unit
        @unit || 'day'
      end

      def units
        (@units || 1).to_i
      end

      def aggregate=(v)
        @aggregate = v.is_a?(TrueClass) || v.to_s == 'yes'
      end

      def from_date_time
        to_date_time - units.send(unit)
      end

      def params=(v)
        @params = Array(v)
      end

      def tuples
        results = []
        params.each_with_index do |q, j|
          result = self.send("#{unit}ly_#{q}", dimension, from_date_time, :to_date => to_date_time, :aggregate => aggregate)
          if result.is_a?(Array) # time range data (non-aggregate)
            result.each_with_index do |r, i|
              results[i] ||= {}
              date_value = r[0][0..2]
              time_value = r[0][3..-1]
              date_time_value = []
              date_time_value << date_value.join('-')
              date_time_value << time_value.join(':') if time_value
              results[i]['raw'] = date_time_value.join(' ').strip
              results[i]['unix'] = Time.mktime(*r[0].map(&:to_i)).to_i
              results[i][q] = r[1]
            end
          elsif result.is_a?(Hash) or result.is_a?(Fixnum) # aggregate data
            results[j] = {q => result}
          end
        end
        results
      end

      private

      def method_missing(meth, *args, &block)
        if meth.to_s =~ /^(minute|hour|dai|day|month|year)ly_([a-z_0-9]+)$/
          granularity = ($1 == 'dai' ? 'day' : $1) + 'ly'
          parameter_name = $2
          data(granularity, parameter_name, *args)
        else
          super
        end
      end

      def parameter_type(parameter_name, dimension)
        RedisAnalytics.redis_connection.hget("#{RedisAnalytics.redis_namespace}:#PARAMETERS", [dimension, parameter_name].join('_'))
      end

      def data(granularity, parameter_name, dimension, from_date, options = {})
        aggregate = options[:aggregate] || false
        x = granularity[0..-3]

        to_date = (options[:to_date] || Time.now).send("end_of_#{x}")
        i = from_date.send("beginning_of_#{x}")

        union = []
        time = []
        begin
          slice_key = i.strftime(FORMAT_SPECIFIER[0..GRANULARITY.index(granularity)].map{|x| x[0]}.join('_'))
          union << "#{RedisAnalytics.redis_namespace}:#{dimension}:#{parameter_name}:#{slice_key}"
          time << slice_key.split('_')
          i += 1.send(x)
        end while i <= to_date
        seq = get_next_seq
        if parameter_type(parameter_name, dimension) == 'String'
          if aggregate
            union_key = "#{RedisAnalytics.redis_namespace}:#{seq}"
            RedisAnalytics.redis_connection.zunionstore(union_key, union)
            RedisAnalytics.redis_connection.expire(union_key, 100)
            return Hash[RedisAnalytics.redis_connection.zrange(union_key, 0, -1, :with_scores => true)]
          else
            return time.zip(union.map{|x| Hash[RedisAnalytics.redis_connection.zrange(x, 0, -1, :with_scores => true)]})
          end
        elsif parameter_type(parameter_name, dimension) == 'Fixnum'
          if aggregate
            return RedisAnalytics.redis_connection.mget(*union).map(&:to_i).inject(:+)
          else
            return time.zip(RedisAnalytics.redis_connection.mget(*union).map(&:to_i))
          end
        else
          if Parameters.public_instance_methods.any?{|m| m.to_s =~ /^#{parameter_name}_ratio_per_(hit|visit)$/}
            aggregate ? {} : time.zip([{}] * time.length)
          elsif Parameters.public_instance_methods.any?{|m| m.to_s =~ /^#{parameter_name}_count_per_(hit|visit)$/}
            aggregate ? 0 : time.zip([0] * time.length)
          else
            aggregate ? 0 : time.zip([0] * time.length)
          end
        end
      end

      def get_next_seq
        seq = RedisAnalytics.redis_connection.incr("#{RedisAnalytics.redis_namespace}:#SEQUENCER")
      end
    end
  end
end
