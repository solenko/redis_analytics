module Rack
  module RedisAnalytics
    class Visit
      include Parameters

      # This class represents one unique visit
      # User may have never visited the site
      # User may have visited before but his visit is expired
      # Everything counted here is unique for a visit

      # helpers
      def for_each_time_range(t)
        RedisAnalytics.redis_key_timestamps.map{|x, y| t.strftime(x)}.each do |ts|
          yield(ts)
        end
      end

      def first_visit_info(dimension = 'global')
        @first_visit_cookie ||= begin
          cookie = @rack_request.cookies[RedisAnalytics.first_visit_cookie_name]
          JSON.parse(cookie) rescue {}
        end || {}
        @first_visit_cookie.fetch(dimension, '').split('.')
      end

      def current_visit_info(dimension = 'global')
        @current_visit_cookie ||= begin
          cookie = @rack_request.cookies[RedisAnalytics.current_visit_cookie_name]
          JSON.parse(cookie) rescue {}
        end || {}
        @current_visit_cookie.fetch(dimension, '').split('.')
      end

      # method used in analytics.rb to initialize visit
      def initialize(request, response, dimensions = [])
        @t = Time.now
        @dimensions = dimensions + ['global']

        @redis_key_prefix = "#{RedisAnalytics.redis_namespace}:"

        RedisAnalytics.redis_connection.sadd("#{@redis_key_prefix}#DIMENSIONS", @dimensions)
        @rack_request = request
        @rack_response = response
      end

      def first_visit_seq(dimension)
        first_visit_info(dimension)[0] || current_visit_info(dimension)[0]
      end

      def current_visit_seq(dimension)
        current_visit_info(dimension)[1]
      end


      def set_current_visit_info(dimension, index, value)
        current_value = current_visit_info(dimension)
        current_value[index] = value
        @current_visit_cookie[dimension] = current_value.join('.')
      end

      def set_first_visit_info(dimension, index, value)
        current_value = first_visit_info(dimension)
        current_value[index] = value
        @first_visit_cookie[dimension] = current_value.join('.')
      end

      def set_current_visit_seq(dimension, value)
        set_current_visit_info(dimension, 1, value)
      end

      def set_first_visit_seq(dimension, value)
        set_first_visit_info(dimension, 0, value)
        set_current_visit_info(dimension, 0, value)
      end

      def first_visit_time(dimension)
        first_visit_info(dimension)[1]
      end

      def last_visit_time(dimension)
        first_visit_info(dimension)[2]
      end

      def last_visit_start_time(dimension)
        current_visit_info(dimension)[2]
      end

      def last_visit_end_time(dimension)
        current_visit_info(dimension)[3]
      end

      # called from analytics.rb
      def record
        puts "Record visit for dimensions #{@dimensions}.inspect"
        @dimensions.each do |dimension|
          fv_seq = first_visit_seq(dimension)
          if current_visit_seq(dimension)
            track("#{dimension}:visit_time", @t.to_i - last_visit_end_time(dimension).to_i)
          else
            set_current_visit_seq(dimension, counter("#{dimension}:visits"))
            track("#{dimension}:visits", 1)
            if fv_seq
              track("#{dimension}:repeat_visits", 1)
            else
              fv_seq = counter("#{dimension}:unique_visits")
              set_first_visit_seq(dimension, fv_seq)
              track("#{dimension}:first_visits", 1)
              track("#{dimension}:unique_visits", fv_seq.to_i)
            end
            exec_custom_methods('visit', dimension)
          end
          exec_custom_methods('hit', dimension)
          track("#{dimension}:page_views", 1)
          track("#{dimension}:second_page_views", 1) if last_visit_start_time(dimension) and (last_visit_start_time(dimension).to_i == last_visit_end_time(dimension).to_i)
        end
        @rack_response
      end

      def exec_custom_methods(type, dimension)
        Parameters.public_instance_methods.each do |meth|
          if m = meth.to_s.match(/^([a-z_]*)_(count|ratio)_per_#{type}$/)
            begin
              return_value = self.send(meth)
              track("#{dimension}:#{m.to_a[1]}", return_value) if return_value
            rescue => e
              warn "#{meth} resulted in an exception #{e}"
            end
          end
        end
      end

      # helpers
      def counter(parameter_name)
        RedisAnalytics.redis_connection.incr("#{@redis_key_prefix}#{parameter_name}")
      end

      def updated_current_visit_info
        # value = [@first_visit_seq, @current_visit_seq, (@last_visit_start_time || @t).to_i, @t.to_i]

        value = @dimensions.inject({}) do |m, d|
          m[d] = [first_visit_seq(d), current_visit_seq(d), (last_visit_start_time(d) || @t).to_i, @t.to_i].join('.')
          m
        end
        expires = @t + (RedisAnalytics.visit_timeout.to_i * 60)
        {:value => JSON.dump(@current_visit_cookie.update(value)), :expires => expires}
      end

      def updated_first_visit_info
        value = @dimensions.inject({}) do |m, d|
          m[d] = [first_visit_seq(d), (first_visit_time(d) || @t).to_i, @t.to_i].join('.')
          m
        end
        expires = @t + (60 * 60 * 24 * 5) # 5 hours

        {:value => JSON.dump(@first_visit_cookie.update(value)), :expires => expires}

        # value = [@first_visit_seq, (@first_visit_time || @t).to_i, @t.to_i]
        #
        # {:value => value.join('.'), :expires => expires}
      end

      def track(parameter_name, parameter_value)
        RedisAnalytics.redis_connection.hmset("#{@redis_key_prefix}#PARAMETERS", parameter_name.gsub(':', '_'), parameter_value.class)
        for_each_time_range(@t) do |ts|
          key = "#{@redis_key_prefix}#{parameter_name}:#{ts}"
          if parameter_value.is_a?(Fixnum)
            RedisAnalytics.redis_connection.incrby(key, parameter_value)
          else
            RedisAnalytics.redis_connection.zincrby(key, 1, parameter_value)
          end
        end
      end

    end
  end
end
