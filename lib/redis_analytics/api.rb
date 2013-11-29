require 'sinatra/base'

module Rack
  module RedisAnalytics

    class Api < Sinatra::Base
      helpers Rack::RedisAnalytics::Helpers

      get '/data/?' do

        begin
          data  = Rack::RedisAnalytics::DataSet.new
          data.dimension = params[:dimension]
          data.to_date_time = params[:to_date_time]
          data.unit = params[:unit]
          data.aggregate = params[:aggregate]
          data.units = params[:unit_count]
          data.params = params[:p].split(',')


          content_type :json
          data.tuples.to_json
        rescue Exception => e
          halt 500, [500, [e.message, e.backtrace]].to_json
        end
      end
    end
  end
end
