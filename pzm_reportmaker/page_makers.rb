require 'logger'
require_relative './budget_calculators'
require_relative './side_effect_transforms'

module PageMakers


def self.set_logger(logger)
  @log = logger
end



# Run erb to expand template in_file and write the html result to out_file
# @return Returns the value passed in as parameter out_file
def self.generate_html(d,in_file,out_file)
  @log.info "Generating #{out_file}"
  out_file = File.absolute_path(out_file)
  pwd = Dir.pwd()
  # Changing into in_file's directory ensures that all relative
  # paths mentioned in in_file work properly
  Dir.chdir(File.dirname(in_file))
  erbed = ERB.new(File.read(File.basename(in_file)))

  File.write(out_file,erbed.result(binding))
  Dir.chdir(pwd)

  return out_file
end



def self.make_pdf(html,header='header.html')
  #return html
  @log.info "Making pdf of #{html}"
  header_in = header #File.absolute_path(header)
  pwd = Dir.pwd()
  Dir.chdir(File.dirname(html))
  html_in = File.basename(html)

  pdfkit = PDFKit.new(File.new("#{html_in}"),
                    :page_size => 'Letter',
                    :margin_left => '2mm',
                    :margin_right => '2mm',
                    :margin_top => '35mm',
                    :margin_bottom => '10mm',
                    :header_html => header_in
                    )

  pdf_file = "#{html_in}.pdf"
  pdfkit.to_file(pdf_file)
  # Undo the chdir from above
  Dir.chdir(pwd)
  # return filename of generated pdf
  return "#{html}.pdf"
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
  return make_pdf(generate_html(d,'template/yield_data.html.erb',
    "report/#{d[:scenario_id]}_yield_data.html"))
end



def self.make_applied_fertilizer(data)
  d = data.clone
  return make_pdf(generate_html(d,'template/applied_fertilizer.html.erb',
                       "report/#{d[:scenario_id]}_applied_fertilizer.html"))
end



def self.make_applied_planting(data)
  d = data.clone
  return make_pdf(generate_html(d,'template/applied_planting.html.erb',
                       "report/#{d[:scenario_id]}_applied_planting.html"))
end



def self.make_yield_by_soil(data)
  d = data.clone
  return make_pdf(generate_html(d,'template/yield_by_soil.html.erb',
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

  return make_pdf(generate_html(d,'template/overall_profit.html.erb',
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

  return make_pdf(generate_html(d,'template/zone_profit.html.erb',
                       "report/#{d[:scenario_id]}_zone_#{d[:mz_id]}_profit.html"))
end



def self.make_overall_revenue_and_expenses(data)
  d = data.clone

  BudgetCalculators.set_overall_expense_revenue_vars(d)

  return make_pdf(generate_html(d,'template/overall_revenue_and_expenses.html.erb',
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

  return make_pdf(generate_html(d,'template/revenue_and_expenses_with_zones.html.erb',
                       "report/#{d[:scenario_id]}_zone_#{d[:mz_id]}_revenue_and_expenses.html"))
end
################################################################################

end # module
