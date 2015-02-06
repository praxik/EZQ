require 'logger'
require 'pdfkit'
require 'erb'
require_relative './budget_calculators'
require_relative './side_effect_transforms'
require_relative './pdf_utils'

module PageMakers


def self.set_logger(logger)
  @log = logger
end



def self.make_number_overlay(num_pages)
  erbed = ERB.new(File.read('template/numbers.tex.erb'))
  File.write('report/page_numbers.tex',erbed.result(binding))
  Dir.chdir('report')
  puts EZQ.exec_cmd("pdflatex -interaction=batchmode page_numbers.tex 1>/dev/null")
  Dir.chdir('..')
end

################################################################################
# each of these needs to return the name of the file it generated

def self.make_yield_data(data)
  d = data.clone
  d[:yield_map] = "#{d[:scenario_id]}_yield.png"
  d[:yield_legend] = "#{d[:scenario_id]}_yield_legend.png"
  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/yield_data.html.erb',
    "report/#{d[:scenario_id]}_yield_data.html"))
end



def self.make_applied_fertilizer(data)
  d = data.clone
  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/applied_fertilizer.html.erb',
                       "report/#{d[:scenario_id]}_applied_fertilizer.html"))
end



def self.make_applied_planting(data)
  d = data.clone
  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/applied_planting.html.erb',
                       "report/#{d[:scenario_id]}_applied_planting.html"))
end



def self.make_yield_by_soil(data)
  d = data.clone
  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/yield_by_soil.html.erb',
                       "report/#{d[:scenario_id]}_yield_by_soil.html"))
end



def self.make_overall_profit(data)
  d = data.clone

  BudgetCalculators.set_overall_expense_revenue_vars(d)

#   d[:field_revenue] = BudgetCalculators.calculate_total_revenue(d[:scenario_budget],d[:field_avg_yield],d[:field_area])
#   d[:field_expenses_per_acre] = BudgetCalculators.calculate_expenses_per_acre(d[:scenario_budget])
#   d[:field_expenses] = d[:field_expenses_per_acre] * d[:field_area]
#   d[:field_profit] = d[:field_revenue] - d[:field_expenses]
#   d[:field_profit_per_acre] = d[:field_profit] / d[:field_area]

  # Image paths
  cid = "#{d[:scenario_id]}"
  d[:field_profit_map] = "profit_map_#{cid}.png"
  d[:field_histogram] = "profit_hist_#{cid}.svg"
  d[:field_pie_chart] = "expenses_pie_#{cid}.svg"


  #SeTransforms.make_expenses_pie_chart(d[:budget_exp],"report/#{d[:field_pie_chart]}")

  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/overall_profit.html.erb',
                       "report/#{d[:scenario_id]}_overall_profit.html"))
end



def self.make_zone_profit(data,zone)
  d = data.clone
  mz = zone.clone

  BudgetCalculators.do_zone_calcs!(d,mz)

  # Image paths
  cid = "#{d[:scenario_id]}_#{d[:mz_id]}"
  d[:mz_profit_map] = "profit_map_#{cid}.png"
  d[:mz_histogram] = "profit_hist_#{cid}.svg"
  d[:mz_pie_chart] = "expenses_pie_#{cid}.svg"

  #SeTransforms.make_expenses_pie_chart(d[:mz_budget_exp],"report/#{d[:mz_pie_chart]}")

  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/zone_profit.html.erb',
                       "report/#{d[:scenario_id]}_zone_#{d[:mz_id]}_profit.html"))
end



def self.make_overall_revenue_and_expenses(data)
  d = data.clone

  BudgetCalculators.set_overall_expense_revenue_vars(d)

  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/overall_revenue_and_expenses.html.erb',
                                  "report/#{d[:scenario_id]}_overall_revenue_and_expenses.html"))
end



def self.make_revenue_and_expenses_with_zones(data,zone)
  d = data.clone
  mz = zone.clone

  BudgetCalculators.do_zone_calcs!(d,mz)

#   if false
#     puts "Zone yield (bu/ac) #{d[:mz_avg_yield]}"
#     puts "Zone area (ac) #{d[:mz_area]}"
#     puts "Zone profit ($/ac) #{d[:mz_profit_per_acre]}"
#     puts "Zone expense ($) #{d[:mz_expenses_per_acre] * d[:mz_area]}"
#     puts "Zone revenue ($) #{d[:mz_revenue_per_acre] * d[:mz_area]}"
#     puts "Zone profit ($) #{d[:mz_profit_per_acre] * d[:mz_area]}"
#     puts "Zone ROI (%) #{d[:mz_profit_per_acre] / d[:mz_expenses_per_acre]}"
#     puts ''
#   end

  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(d,'template/revenue_and_expenses_with_zones.html.erb',
                       "report/#{d[:scenario_id]}_zone_#{d[:mz_id]}_revenue_and_expenses.html"))
end



def self.make_toc(input,reports)
  @log.debug "PageMakers.make_toc begin"
  field_names = []
  scenario_names = []
  scenario_years = []
  input.each do |field,|
    scenario_names += field['scenarios'].map{|s| s['name']}
    scenario_years += field['scenarios'].map{|s| s['year'].to_s}
    field_names += Array.new(field['scenarios'].size,field['name'])
  end
  @log.debug "scenario_names: #{scenario_names}"
  @log.debug "scenario_years: #{scenario_years}"
  @log.debug "field_names: #{field_names}"
  @log.debug "reports: #{reports}"
  # Do a reduce-with-partials operation the array of page numbers from each
  # sub-report.
  start_pages = reports.flatten.map{|f| AgPdfUtils.get_num_pages(f)}.
                  reduce([1]){|acc,pgs| acc += acc.last ? [acc.last + pgs] : [pgs]}.
                  map{|p| p.to_s}
  @log.debug "start_pages: #{start_pages}"
  tokens = field_names.zip(scenario_names,scenario_years,start_pages)
  @log.debug "tokens: #{tokens}"
  # creates format ["field_name: scenario_name: year",page]
  entries = tokens.map{|t| ["#{t[0]}: #{t[1]}: #{t[2]}",t[3]]}
  @log.debug "entries: #{entries}"
  @log.debug "PageMakers.make_toc end"

  return AgPdfUtils.html_to_pdf(AgPdfUtils.generate_html(entries,'template/toc.html.erb',
                       "report/toc.html"))
end
################################################################################

end # module
