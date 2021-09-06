require "hypofriend_demo/version"
require 'activerecord'
require 'activesupport'
require 'activerecord-import'
require 'pg'
require 'fast_jsonparser'
require 'open-uri'
require 'sidekiq'

# Architecture TODO

# Should Fetcher class for check and get raw data
# Should Parser class for leads processing
# Should TimeStampCreator for data savings
# Should JobCreator for job performs

# Next steps for implement

# Should use rubocop for check styling
# Should use https://github.com/DamirSvrtan/fasterer
# Should use https://github.com/palkan/faqueue code example for multi tenant apps https://github.com/palkan/faqueue#about
# Should check https://github.com/Shopify/job-iteration for job size more than 1M
# Should do not disable GC when calculating metrics
# Should disable the profiler when calculating metrics
# Should invest in the convenience of development and speed of the feedback loop!
# Should implement tests for perfomance and add time frame for the tests
# Should use ruby-prof, stackprof and progress bar for continue optimizing
# Should use ractors(ruby-next) or threads for improving performance
# Should use https://github.com/palkan/isolator

# Time spent - 2.5 hours

module HypofriendDemo
  class Error < StandardError; end

  class App
    IMPORT_URL = "https://offer-v3.hypofriend.de/api/v5/new-offers?loan_amount=350000&property_value=350000&repayment=1.0&years_fixed=15"
    TTME_DISTRIBUTION = 60
    SLICE_COUNTER = 100

    def perform
      lines = FastJsonparser.load(open(IMPORT_URL).read)
      leads = perform_lines(lines)
      ids = create_timestamps(leads)
      perform_jobs(ids)
    end

    private

    def perform_jobs(ids)
      time_now = Time.now.in_time_zone('Europe/Berlin')
      time_index = 0

      ids.each_slice(SLICE_COUNTER) do |ids_banch|
        time_index == TTME_DISTRIBUTION ? time_index = 0 : time_index += 1
        Sidekiq::Client.push_bulk(class: PropertyWorkerMailer, args: ids_banch.map { |id| [id]}, 
        wait_until: time_now.change(hour: 9, min: time_index), queue: 'email_blasts' })
      end
    end  

    def create_timestamps
      ids = []

      leads.each_slice(SLICE_COUNTER).times do |leads_banch|
        temp_records = []
        leads_banch.each do |record|
          temp_records << record.email_timestamps.create(mailing: 'property_radar')
        end
        
        leads = Lead.import temp_records
        ids << temp_records.pluck(:id)
      end
      ids
    end

    def perform_lines(lines)
      # Implement fast json parser loader
      # also we can use threads or ractors for implement more scaleable parsing file feature(chunk file and write into db)
      # for example (chunked part of file)
      # ActiveRecord::Base.transaction do
      #   trips_command =
      #     "copy trips (from_id, to_id, start_time, duration_minutes, price_cents, bus_id) from stdin with csv delimiter ';'"
      
      #   ActiveRecord::Base.connection.raw_connection.copy_data trips_command do
      #     File.open(file_name) do |ff|
      #       nesting = 0
      #       str = +""
      
      #       while !ff.eof?
      #         ch = ff.read(1)
      #         case
      #         when ch == '{'
      #           nesting += 1
      #           str << ch
      #         when ch == '}'
      #           nesting -= 1
      #           str << ch
      #           if nesting == 0
      #             trip = Oj.load(str)
      #             import(trip)
      #             progress_bar.increment
      #             str = +""
      #           end
      #         when nesting >= 1
      #           str << ch
      #         end
      #       end
      #     end
      #   end
      # end
      
      # def import(trip)
      #   from_id = @cities[trip['from']]
      #   if !from_id
      #     from_id = cities.size + 1
      #     @cities[trip['from']] = from_id
      #   end
      
      #   # ...
      
      #   # Streaming a prepared data chunk into postgres
      #   connection.put_copy_data("#{from_id};#{to_id};#{trip['start_time']};#{trip['duration_minutes']};#{trip['price_cents']};#{bus_id}\n")
      # end
    end  
  end
end
