# -*- coding: utf-8 -*-
require 'digest/md5'
module Rack
  module RedisAnalytics
    class Analytics

      def initialize(app)
        @app = app
      end

      def call(env)
        dup.call!(env)
      end

      def call!(env)
        @env = env
        @request  = Request.new(env)
        status, headers, body = @app.call(env)
        @response = Rack::Response.new(body, status, headers)
        record if should_record?
        @response.finish
      end

      def should_record?
        return false unless @response.ok?
        return false unless correct_content_type?
        RedisAnalytics.path_filters.each do |filter|
          return false if filter.matches?(@request.path)
        end
        RedisAnalytics.filters.each do |filter|
          return false if filter.matches?(@request, @response)
        end
        return true
      end

      def record
        v = Visit.new(@request, @response, RedisAnalytics.dimensions(@request))
        @response = v.record
        @response.set_cookie(RedisAnalytics.current_visit_cookie_name, v.updated_current_visit_info)
        @response.set_cookie(RedisAnalytics.first_visit_cookie_name, v.updated_first_visit_info)
      end

      def correct_content_type?
        formats = ['text/html']
        formats += ['application/json', 'text/javascript'] if RedisAnalytics.track_ajax_calls
        @response.content_type =~ /^[#{formats.join('|')}]/ && !@request.path.starts_with?('/assets')
      end
    end
  end
end
