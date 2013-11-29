module Rack
  module RedisAnalytics
    module Helpers



      private


      def time_range
        (request.cookies["_rarng"] || RedisAnalytics.default_range).to_sym
      end
    end
  end
end
