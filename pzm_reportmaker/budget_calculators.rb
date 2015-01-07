# Functions in this file calculate relevant values from budgets.


module BudgetCalculators

def self.do_zone_calcs!(d,mz)
  d[:mz_name] = mz['name']
  d[:mz_id] = mz['id']
  d[:mz_area] = mz['get_area_in_acres']
  d[:mz_commodity_price] = get_commodity_price(mz['budget']['budget_items'])

  #set_overall_expense_revenue_vars(d)

  d[:mz_avg_yield] = mz['get_target_zone_yield']
  d[:mz_expenses_per_acre] = calculate_expenses_per_acre(mz['budget'])
  d[:mz_expenses] = d[:mz_expenses_per_acre] * d[:mz_area]
  d[:mz_other_revenue_per_acre] = calculate_other_revenue_per_acre(mz['budget']['budget_items'])
  d[:mz_revenue] = (d[:mz_commodity_price] * d[:mz_avg_yield] + d[:mz_other_revenue_per_acre]) * d[:mz_area]
  d[:mz_revenue_per_acre] = d[:mz_revenue] / d[:mz_area]
  d[:mz_profit_per_acre] = d[:mz_revenue_per_acre] - d[:mz_expenses_per_acre]
  d[:mz_profit] = d[:mz_revenue] - d[:mz_expenses]
  d[:mz_roi] = d[:mz_profit_per_acre] / d[:mz_expenses_per_acre] * 100
  d[:mz_year] = d[:scenario_budget]['name']

  d[:mz_budget_exp] = sort_budget(mz['budget']['budget_items'],'item_id')
                                   .delete_if{|it| !it['expense']}
  return nil
end



  def self.calculate_other_revenue_per_acre(budget_items)
  return budget_items.reduce(0) do|orev,bi|
    orev += (!bi['expense'] && bi['unit_id'] == 1) ? bi['amount'] : 0
  end
end


def self.calculate_total_revenue(budget,yld,area)
  items = budget['budget_items']
  commodity_price = get_commodity_price(items)
  other_rev = calculate_other_revenue_per_acre(items)
  return area * ( commodity_price * yld + other_rev)
end



def self.calculate_expenses_per_acre(budget)
  expenses = budget['budget_items'].select{|bi| bi['expense'] == true}
                    .map{|i| i['amount']}.reduce(:+)
  expenses = 1.0 if !expenses or expenses == 0.0
  return expenses
end



def self.get_commodity_price(budget_items)
  budget_items.select{|bi| bi['item_name'] == 'Commodity Price'}[0]['amount']
end



def self.zone_weighted_expenses(zones)
  return zones.map do |z|
    area = z['get_area_in_acres']
    calculate_expenses_per_acre(z['budget'])*area*area
  end.reduce(:+)
end



def self.zone_weighted_revenue(zones)
  return zones.map do |z|
    budget = z['budget']
    yld = z['get_target_zone_yield']
    area = z['get_area_in_acres']
    calculate_total_revenue(budget,yld,area)*area
  end.reduce(:+)
end



def self.zone_total_area(zones)
  # Experimenting with style here. Both versions return same value.
  #return zones.map{|z| z['get_area_in_acres']}.reduce(:+)
  return zones.reduce(0){|area,z| area += z['get_area_in_acres']}
end


def self.set_overall_expense_revenue_vars(d)
  zw_expenses = zone_weighted_expenses(d[:zones]) || 0
  zw_revenue = zone_weighted_revenue(d[:zones]) || 0
  zt_area = zone_total_area(d[:zones]) || 0
  field_area = d[:field_area]
  nz_area = field_area - zt_area

  price = get_commodity_price(d[:scenario_budget]['budget_items'])

  # expenses
  fe = ( calculate_expenses_per_acre(d[:scenario_budget]) * nz_area**2 + zw_expenses ) / field_area
  fepa = fe / field_area

  # revenue
  orpa = calculate_other_revenue_per_acre(d[:scenario_budget]['budget_items'])
  yld = d[:nz_yield] # Note this is field yield, *not* the weighted yield
  fr = ( (orpa + price * yld) * nz_area**2 + zw_revenue ) / field_area
  frpa = fr / field_area

  # profit
  fp = fr - fe

  d[:commodity_price] = price
  d[:field_expenses_per_acre] = fepa
  d[:other_revenue_per_acre] = orpa
  d[:field_revenue_per_acre] = frpa

  d[:field_expenses] = fe
  d[:field_revenue] = fr
  d[:field_profit] = fp
  d[:field_profit_per_acre] = frpa - fepa

  d[:field_roi] = 100.0 * fp / fe

  if true
    puts "Comm Price: #{price}"
    puts "ORPA:     #{orpa}"
    puts "Expenses: #{fe}"
    puts "Revenue:  #{fr}"
    puts "Profit:   #{fp}"
    puts "PPA:      #{frpa-fepa}"
    puts "Area:     #{field_area}"
    puts "NZArea:   #{nz_area}"
  end

  # We want a sorted budget that includes only expenses
  d[:budget_exp] = sort_budget(d[:scenario_budget]['budget_items'],'item_id')
                    .select{|it| it['expense'] == true}
  return nil
end



# Sort the budget items by item_id so they can be displayed in the correct order
# with minimal fuss in the erb. Will also sort sub_items and strip them out
# if sum(subs) != parent amount
def self.sort_budget(budget_items,key)
  sorted = budget_items.sort_by{|it| it[key]}
  sorted.each do |bi|
    if bi.fetch('sub_budget_items',[]).size > 0
      # Can't rely on strict equality since these are rounded floating point numbers.
      if (bi['sub_budget_items'].map{|sbi| sbi['amount']}.reduce(:+) - bi['amount']).abs() < 0.001
        bi['sub_budget_items'] = sort_budget(bi['sub_budget_items'],key)
      else
        bi['sub_budget_items'] = []
      end
    end
  end
  return sorted
end

end # module
