import random
from psycopg2.extensions import AsIs
from scipy import stats
import numpy as np

#support function to obtain agent dataframe
def agent_to_data_frame(agent_name, db):
    df_agents = db.table_to_df('agents')
    df = df_agents[df_agents['agent_name'] == agent_name]
    df = df.pivot(index='agent_id', columns = 'attribute', values = 'value')
    df = df.drop('Country', 1)
    df = df.drop('Latitude', 1)
    df = df.drop('Longitude', 1)
    df = df.reset_index(drop=False)
    df.columns.name = None
    return df

#Demand generation
def generate_demand(mu, sigma):
        
    lower, upper = 0, 9999999

    #instantiate an object X using the above four parameters,
    X = stats.truncnorm((lower - mu) / sigma, (upper - mu) / sigma, loc=mu, scale=sigma)

    return int(X.rvs(1))

#For disruptions there are 3 states, which are 0, 1 and 2
#From each state there is a propability alfa that it will move to
#State can be changed only once per day
def simulate_disruption(Disruption_level, alfa):

    Disruption_updated = 0
    
    if Disruption_level == 0:

        if Disruption_updated == 0:
            if random.uniform(0, 1) < alfa:
                Disruption_level = 1

                Disruption_updated = 1

    elif Disruption_level == 1:

        if Disruption_updated == 0:
            if random.uniform(0, 1) < alfa:
                Disruption_level = 2

                Disruption_updated = 1

    else:

        if Disruption_updated == 0:
            if random.uniform(0, 1) < alfa:
                Disruption_level = 0

                Disruption_updated = 1

    return Disruption_level

#Calculate max lead time to determine days of estimation
def calculate_max_lead_time(db):

    #Get max lead time of suppliers
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Supplier']
    df_agents = df_agents[df_agents['attribute'] == 'Lead_time']
    max_supplier_LD = int(df_agents['value'].astype(int).max())

    #get max lead tiems of assembly plant
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Assembly plant']
    df_agents = df_agents[df_agents['attribute'] == 'Production_duration']
    max_assembly_LD = int(df_agents['value'].astype(int).max())

    #get max lead time of warehousing
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Warehouse']
    df_agents = df_agents[df_agents['attribute'] == 'Operation_duration']
    max_warehouse_LD = int(df_agents['value'].astype(int).max())

    #Sum total lead time
    max_LD = max_supplier_LD + max_assembly_LD + max_warehouse_LD

    return max_LD

#Calculate max lead time to determine days of estimation
def calculate_max_lead_time_for_short_route(db):

    #Get max lead time of suppliers
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Supplier']
    df_agents = df_agents[df_agents['attribute'] == 'Lead_time']
    df_agents['Country'] = df_agents['agent_id'].str.split('_',expand=True)[1]
    df_agents = df_agents[df_agents['Country'] != 'C']
    max_supplier_LD = int(df_agents['value'].astype(int).max())

    #get max lead tiems of assembly plant
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Assembly plant']
    df_agents = df_agents[df_agents['attribute'] == 'Production_duration']
    max_assembly_LD = int(df_agents['value'].astype(int).max())

    #Sum total lead time
    max_LD = max_supplier_LD + max_assembly_LD

    return max_LD

def get_lead_time(db):

    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Supplier']
    df_agents = df_agents[df_agents['attribute'] == 'Lead_time']
    supplier_LD = df_agents['value'].tolist()

    #get max lead time of assembly plant
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Assembly plant']
    df_agents = df_agents[df_agents['attribute'] == 'Production_duration']
    assembly_LD = df_agents['value'].tolist()

    #get max lead time of warehousing
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Warehouse']
    df_agents = df_agents[df_agents['attribute'] == 'Operation_duration']
    warehouse_LD = df_agents['value'].tolist()

    full_LD = supplier_LD + assembly_LD + warehouse_LD
    full_LD = [ int(x) for x in full_LD ]

    return full_LD

#calculates the mean value of a indicator per day, by ignoring agent_id and/or raw material type
def get_mean_indicator_by_day(db, day, table_name, indicator):

        df = db.table_to_df(table_name)
        df = df[['day', indicator]]
        df = df[df['day'] == day]
        mean_indicator = df[indicator].mean()

        return mean_indicator

#calculates mean value of a indicator per day, for a spefic order type e.g. finished product, or raw material type
def get_mean_indicator_by_day_order(db, day, table_name, indicator):

    df = db.table_to_df(table_name)
    df = df[['day', indicator]]
    df = df[df['day'] == day]
    df = df.dropna()
    mean_indicator = df[indicator].mean()

    return mean_indicator

#Takes the value of a KPI for ploting purpose
def to_report_KPI(model, db, day, KPI_indicator):

    day = day - 1
    
    Indicator = db.select_where('SELECT (%s) FROM kpi_table WHERE day = (%s)', (AsIs(KPI_indicator), day))
    if len(Indicator) != 0:
        Indicator = Indicator[0][0]
        if KPI_indicator != 'Order_fulfilment':
            Indicator = int(Indicator)
    elif len(Indicator) == 0:
        Indicator = 0

    return Indicator

def to_report_LD(model, db, day, route_type):
    
    df = db.table_to_df('lead_time_history')
    df = df[df['day'] == day]
    df['Country'] = df['supplier_id'].str.split('_',expand=True)[1]

    long_route_LD = df[df['Country'] == 'C']
    long_route_LD = long_route_LD['lead_time'].tolist()
    long_route_LD = sum(long_route_LD) / len(long_route_LD)

    short_route_LD = df[df['Country'] != 'C']
    short_route_LD = short_route_LD['lead_time'].tolist()
    short_route_LD = sum(short_route_LD) / len(short_route_LD)

    #get max lead tiems of assembly plant
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Assembly plant']
    df_agents = df_agents[df_agents['attribute'] == 'Production_duration']
    max_assembly_LD = df_agents['value'].max()

    #get max lead time of warehousing
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Warehouse']
    df_agents = df_agents[df_agents['attribute'] == 'Operation_duration']
    max_warehouse_LD = df_agents['value'].max()

    long_route_LD = int(long_route_LD) + int(max_warehouse_LD) + int(max_assembly_LD)

    short_route_LD = int(short_route_LD) + int(max_warehouse_LD) + int(max_assembly_LD)

    if route_type == 'short route':
        Indicator = short_route_LD

    if route_type == 'long route':
        Indicator = long_route_LD

    return Indicator

def select_database_title(Inventory_strategy, Optimization_goal):
    
    if (Inventory_strategy == 'Nayve estimation') & (Optimization_goal == 'Profit'):
        database_title = 'scenario_1'
    if (Inventory_strategy == 'Nayve estimation') & (Optimization_goal == 'Lead-time'):
        database_title = 'scenario_2'
    if (Inventory_strategy == 'Nayve estimation') & (Optimization_goal == 'On time delivery'):
        database_title = 'scenario_3'
    if (Inventory_strategy == 'Nayve estimation and machine learning for buffe size') & (Optimization_goal == 'Profit'):
        database_title = 'scenario_4'
    if (Inventory_strategy == 'Nayve estimation and machine learning for buffe size') & (Optimization_goal == 'Lead-time'):
        database_title = 'scenario_5'
    if (Inventory_strategy == 'Nayve estimation and machine learning for buffe size') & (Optimization_goal == 'On time delivery'):
        database_title = 'scenario_6'
    if (Inventory_strategy == 'DDMRP') & (Optimization_goal == 'Profit'):
        database_title = 'scenario_7'
    if (Inventory_strategy == 'DDMRP') & (Optimization_goal == 'Lead-time'):
        database_title = 'scenario_8'
    if (Inventory_strategy == 'DDMRP') & (Optimization_goal == 'On time delivery'):
        database_title = 'scenario_9'

    return database_title

#Update the main key performance indicators in to the table
def calculate_KPI(self, day, db, price_per_unit):
    
    total_orders = int(get_mean_indicator_by_day(db, day, 'retail_inventory', 'order_size'))

    total_stock_level = int(get_mean_indicator_by_day(db, day, 'retail_inventory', 'stock_level'))

    backlog = int(get_mean_indicator_by_day(db, day, 'retail_inventory', 'backlog'))

    total_storage_costs = int(get_mean_indicator_by_day(db, day, 'retail_inventory', 'storage_costs'))

    total_revenue = int(get_mean_indicator_by_day(db, day, 'retail_inventory', 'revenue'))

    order_fulfilment = round((total_orders - backlog) / total_orders, 2)

    total_cost_of_production = int(get_mean_indicator_by_day(db, day, 'production_inventory', 'costs_of_production'))
    #divide by the number of suppliers per assembly plant
    total_cost_of_production = int(total_cost_of_production / 5)

    total_cost_of_raw_materials = get_mean_indicator_by_day_order(db, day, 'raw_material_orders', 'costs_of_raw_materials')

    if np.isnan(total_cost_of_raw_materials) == False:
        total_cost_of_raw_materials = int(total_cost_of_raw_materials)
    else:
        total_cost_of_raw_materials = 0

    total_cost_of_raw_materials_transportation = get_mean_indicator_by_day_order(db, day, 'raw_material_orders', 'cost_of_transportation')

    if np.isnan(total_cost_of_raw_materials_transportation) == False:
        total_cost_of_raw_materials_transportation = int(total_cost_of_raw_materials_transportation)
    else:
        total_cost_of_raw_materials_transportation = 0

    total_cost_of_products_transportation = get_mean_indicator_by_day_order(db, day, 'product_orders', 'cost_of_transportation')

    if np.isnan(total_cost_of_products_transportation) == False:
        total_cost_of_products_transportation = int(total_cost_of_products_transportation)
    else:
        total_cost_of_products_transportation = 0

    total_costs_of_transportation = int(total_cost_of_raw_materials_transportation + total_cost_of_products_transportation)

    total_costs = total_cost_of_production + total_cost_of_raw_materials + total_costs_of_transportation + total_storage_costs

    total_profit = total_revenue - total_costs

    db.insert_to_table('INSERT INTO KPI_table (Day, Total_orders, Total_inventory, Backlog, Order_fulfilment, Revenue, Cost_of_production, Costs_of_raw_material, Cost_of_delivery, costs_of_storage, Profit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (day, total_orders, total_stock_level, backlog, order_fulfilment, total_revenue, total_cost_of_production, total_cost_of_raw_materials, total_costs_of_transportation, total_storage_costs, total_profit))
