module Rack
  module RedisAnalytics
    class Analytics
      
      def initialize(app)
        @app = app
      end
      
      def call(env)
        t0 = Time.now
        @request = Rack::Request.new(env)
        # call the @app
        status, headers, body = @app.call(env)

        # create a response
        @response = Rack::Response.new body, status, headers
        
        t = Time.now
        if visit = @request.cookies[Rack::RedisAnalytics.returning_user_cookie_name]
          track_recent_visit(t)
        else
          unless track_recent_visit(t)
            [t.strftime("%Y"), t.strftime("%Y_%m"), t.strftime("%Y_%m_%d"), t.strftime("%Y_%m_%d_%H")].each do |ts|
              Rack::RedisAnalytics.redis_connection.incr("NEW_VISITS_#{ts}")
            end
          end
        end

        # write the response
        @response.finish
      end
      
      def track_recent_visit(t)
        visit_start_time, visit_end_time = t.to_i
        if recent_visit = @request.cookies[Rack::RedisAnalytics.visit_cookie_name]
          # recent visit was made
          seq, visit_start_time, visit_end_time = recent_visit.split('.')
          # add to total visit time
          [t.strftime("%Y"), t.strftime("%Y_%m"), t.strftime("%Y_%m_%d"), t.strftime("%Y_%m_%d_%H")].each do |ts|
            Rack::RedisAnalytics.redis_connection.incrby("TOTAL_VISIT_TIME_#{ts}", t.to_i - visit_end_time.to_i)
          end
        else
          # no recent visit
          [t.strftime("%Y"), t.strftime("%Y_%m"), t.strftime("%Y_%m_%d"), t.strftime("%Y_%m_%d_%H")].each do |ts|
            # increment the total visits
            seq = Rack::RedisAnalytics.redis_connection.incr("TOTAL_VISITS_#{ts}")
            # add to total unique visits
            Rack::RedisAnalytics.redis_connection.sadd("UNQ_VISITS_#{ts}", seq)

            # add ua info
            # ua = UserAgent.parse(env['HTTP_USERAGENT'])
            # Rack::RedisAnalytics.redis_connection.incr("UA_B_#{ua.browser}_#{ts}")
            # Rack::RedisAnalytics.redis_connection.incr("UA_B_V_#{ua.browser}_#{ua.version}_#{ts}")
            # Rack::RedisAnalytics.redis_connection.incr("UA_P_#{ua.platform}_#{ts}")
            # Rack::RedisAnalytics.redis_connection.incr("UA_M_#{ts}") if ua.mobile?
            
          end
        end
        # simply update the visit_start_time and visit_end_time
        @response.set_cookie(Rack::RedisAnalytics.visit_cookie_name, {:value => "#{seq}.#{visit_start_time}.#{t.to_i}", :expires => t + (Rack::RedisAnalytics.visit_timeout.to_i * 60 )})

        # create the permanent cookie (2 years)
        @response.set_cookie(Rack::RedisAnalytics.returning_user_cookie_name, {:value => "RedisAnalytics - copyright Schubert Cardozo - 2013 - http://www.github.com/saturnine/redis_analytics", :expires => t + (2 * 365 * 24 * 60 * 60)})

        puts "VISIT = #{seq} [#{t}]"

        # return the sequencer
        recent_visit
      end
      
    end
  end
end