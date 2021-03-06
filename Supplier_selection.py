from sklearn.naive_bayes import GaussianNB
import pandas as pd
import numpy as np

import sys

from Support_functions import calculate_max_lead_time, agent_to_data_frame, calculate_max_lead_time_for_short_route

#Input retailer and list of suppliers ID
#Output discount for suppliers based on their previous purchase history
def set_discount(db, selected_retailer, supplier_id_R_list):

    df_raw_material_orders = db.table_to_df('raw_material_orders')
    df_raw_material_orders = df_raw_material_orders[['agent_id', 'day', 'current_order_from']]
    df_raw_material_orders = df_raw_material_orders[df_raw_material_orders['agent_id'] == selected_retailer]
    df_raw_material_orders = df_raw_material_orders[df_raw_material_orders['current_order_from'].isin(supplier_id_R_list)]

    if df_raw_material_orders.empty == False:
        
        all_discounts = pd.DataFrame([])

        for selected_supplier in df_raw_material_orders['current_order_from'].unique():
            df_active = df_raw_material_orders[df_raw_material_orders['current_order_from'] == selected_supplier]
            df_active = df_active.tail(5)
            consecetive_purchase = df_active['current_order_from'].tolist().count(selected_supplier)
            Discount = 1 - ((consecetive_purchase * 5) / 100)
            Discount = pd.DataFrame({'agent_id': selected_supplier, 'Discount': Discount})
            all_discounts = Discount.append(Discount)
    
    else:
        supplier_id_R_list.append(selected_retailer)
        all_discounts = pd.DataFrame({'agent_id': supplier_id_R_list})
        all_discounts['Discount'] = 1

    return all_discounts

#Generate lsit of all suppliers for selected retailer
def retrieve_relevant_supplier_list(db, retailer_id):

    #Obtain dataframe of all ID
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Supplier']
    supplier_id = df_agents[['agent_id']]
    supplier_id['Country'] = supplier_id['agent_id'].str.split('_',expand=True)[1]
    
    #Select the country of the retailer
    retailer_country = retailer_id.split("_")[1]
    
    #Select all supplier ID of retailer country
    supplier_id = supplier_id[(supplier_id['Country'] == retailer_country)]
    supplier_id = list(set(supplier_id['agent_id'].tolist()))

    return supplier_id

#Retrieve suppler ID of off-shore counrty (in this cas China)
def retrieve_foreign_supplier_list(db, Country):
    
    #Obtain dataframe of all ID
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Supplier']
    supplier_id = df_agents[['agent_id']]
    supplier_id['Country'] = supplier_id['agent_id'].str.split('_',expand=True)[1]
    
    #Select all supplier ID of retailer country and china
    supplier_id = supplier_id[supplier_id['Country'] == Country]
    supplier_id = list(set(supplier_id['agent_id'].tolist()))

    return supplier_id

def retrieve_relevant_assembly_list(db, retailer_id):
    
    #Obtain dataframe of all ID
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Assembly plant']
    assembly_id = df_agents[['agent_id']]
    assembly_id['Country'] = assembly_id['agent_id'].str.split('_',expand=True)[1]
    
    #Select the country of the retailer
    retailer_country = retailer_id.split("_")[1]
    
    #Select all supplier ID of retailer country and china
    assembly_id = assembly_id[assembly_id['Country'] == retailer_country]
    assembly_id = list(set(assembly_id['agent_id'].tolist()))

    return assembly_id

def retrieve_relevant_warehouse_list(db, retailer_id):
    
    #Obtain dataframe of all ID
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_name'] == 'Warehouse']
    warehouse_id = df_agents[['agent_id']]
    warehouse_id = list(set(warehouse_id['agent_id'].tolist()))

    return warehouse_id

#get all orders and status if the order was delivered on time or not
def get_orders_and_on_time(retailer_id, supplier_list, db):
        
    assembly_id_list = retrieve_relevant_assembly_list(db, retailer_id)

    df_raw_material_orders = db.table_to_df('raw_material_orders')
    df_raw_material_orders = df_raw_material_orders[df_raw_material_orders['agent_id'].isin(assembly_id_list)]
    df_raw_material_orders = df_raw_material_orders[df_raw_material_orders['current_order_from'].isin(supplier_list)]
    df_raw_material_orders = df_raw_material_orders['order_id'].tolist()

    df_product_orders = db.table_to_df('product_orders')
    df_product_orders = df_product_orders[df_product_orders['agent_id'] == retailer_id]
    df_product_orders = df_product_orders['order_id'].tolist()

    all_orders = df_raw_material_orders + df_product_orders

    df_schedule_information = db.table_to_df('schedule_information')
    df_schedule_information = df_schedule_information[df_schedule_information['order_id'].isin(all_orders)]
    df_schedule_information = df_schedule_information[['order_id', 'day_planned', 'day_actual', 'stop_id', 'on_time']]

    df_schedule_information = df_schedule_information.fillna(1)

    return df_schedule_information

#estimate on time status based naive bayse
#later check to calculate propability from 0 to 1, and not final label
def predict_on_time_status(db, supplier_id, test, selected_retailer):
        
    df_schedule_information = get_orders_and_on_time(selected_retailer, supplier_id, db)

    if df_schedule_information.empty == False:
        gnb = GaussianNB()
        X = df_schedule_information[['day_planned', 'stop_id']]
        X['stop_id'] = pd.Categorical(X['stop_id'])
        X['stop_id_codes'] =  X['stop_id'].cat.codes
        Y = df_schedule_information['on_time']
        gnb = gnb.fit(X[['day_planned', 'stop_id_codes']], Y)

        total = 0

        test = test.merge(X[['stop_id_codes', 'stop_id']], on = 'stop_id')

        for _, row in test.iterrows():

            on_time_estimate = gnb.predict(np.array(row[['Day_planned', 'stop_id_codes']]).reshape(1,-1))

            if on_time_estimate == 1:
                total = total + 1

        total = total / len(test['stop_id'])

    else:

        total = 1

    return total

#Receive duration between nodes x and y
def get_duration(db, x, y):

    if isinstance(x, list) == False: 
        x = [x]

    if isinstance(y, list) == False: 
        y = [y]

    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(x)]
    df_edge = df_edge[df_edge['node_y_id'].isin(y)]
    
    return df_edge['duration'].values.astype(int)

#receive distance between nodes x and y
def get_distance(db, x, y):

    if isinstance(x, list) == False: 
        x = [x]

    if isinstance(y, list) == False: 
        y = [y]

    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(x)]
    df_edge = df_edge[df_edge['node_y_id'].isin(y)]
    
    return df_edge['distance'].values.astype(int)

def select_supplier_costs(db, day, selected_retailer, short_route_cost, long_route_cost):
    
    #Get estiamted demand from retail inventory of all agents
    df_retailer_inventory = db.table_to_df('retail_inventory')
    df_retailer_inventory = df_retailer_inventory[df_retailer_inventory['day'] >= day]

    #get inventory table of specific retailer
    df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]

    #Create a list of demand
    product_demand = df_active['estimated_order'].tolist()

    #In this case, the maximum lead time from china is 15 days
    #While the maximum lead time from local producer is 5 days
    #Thus comparison of 1 long order with 3 short orders are made

    ##################################################
    ########### Calculation of short order ###########
    ##################################################

    #retrieve supplier ID of retailer origin country
    supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)
    assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)

    df_supplier = agent_to_data_frame('Supplier', db)
    df_supplier = df_supplier[df_supplier['agent_id'].isin(supplier_id_R_list)]
    df_supplier = df_supplier[['agent_id', 'Type_of_raw_material', 'Price']]

    #retrieve list of raw materials
    df_BOM = db.table_to_df('bill_of_material')
    df_BOM = df_BOM[['raw_material_id', 'quantity']]

    #add quantity of parts needed
    df_supplier = df_supplier.merge(df_BOM, how = 'left', left_on = 'Type_of_raw_material', right_on = 'raw_material_id')

    #calcualte parts needed based on product demand
    #the schedule is planned for the maximum period, thus the product demand is analyzed as the sum for the period
    df_supplier['product_demand'] = sum(product_demand)
    df_supplier['parts_demand'] = df_supplier['quantity'] * df_supplier['product_demand']

    #calculate price of raw materials
    Discounts = set_discount(db, selected_retailer, supplier_id_R_list)
    df_supplier = df_supplier.merge(Discounts, how = 'left', on = 'agent_id')
    df_supplier['Price'] = df_supplier['Price'] * df_supplier['Discount']

    #get table of edges with weight as distance
    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(supplier_id_R_list)]
    df_edge = df_edge[df_edge['node_y_id'].isin(assembly_id_list)]
    df_edge = df_edge[['node_x_id','distance']]

    #Append to original dataframe distance
    df_supplier = df_supplier.merge(df_edge, how = 'left', left_on = 'agent_id', right_on = 'node_x_id')

    #Increase distance by the number of deliveries (in this case 3)
    df_supplier['distance'] = df_supplier['distance'] * 3

    #Calculate total costs
    df_supplier['costs_of_raw_material'] = df_supplier['Price'].astype(float) * df_supplier['parts_demand']
    costs_of_raw_material = df_supplier['costs_of_raw_material'].sum()
    df_supplier['total_costs_of_transportation'] = df_supplier['distance'] * short_route_cost
    total_costs_of_transportation = df_supplier['total_costs_of_transportation'].sum()

    total_costs_of_short_order = costs_of_raw_material + total_costs_of_transportation
    
    ##################################################
    ########### Calculation of long order ###########
    ##################################################

    #retrieve supplier ID of china 
    supplier_id_C_list = retrieve_foreign_supplier_list(db, 'C')
    assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)
    warehouse_id_list = retrieve_relevant_warehouse_list(db, selected_retailer)

    df_supplier = agent_to_data_frame('Supplier', db)
    df_supplier = df_supplier[df_supplier['agent_id'].isin(supplier_id_C_list)]
    df_supplier = df_supplier[['agent_id', 'Type_of_raw_material', 'Price']]

    #retrieve list of raw materials
    df_BOM = db.table_to_df('bill_of_material')
    df_BOM = df_BOM[['raw_material_id', 'quantity']]

    #add quantity of parts needed
    df_supplier = df_supplier.merge(df_BOM, how = 'left', left_on = 'Type_of_raw_material', right_on = 'raw_material_id')

    #calcualte parts needed based on product demand
    #the schedule is planned for the maximum period, thus the product demand is analyzed as the sum for the period
    df_supplier['product_demand'] = sum(product_demand)
    df_supplier['parts_demand'] = df_supplier['quantity'] * df_supplier['product_demand']

    #calculate price of raw materials
    Discounts = set_discount(db, selected_retailer, supplier_id_C_list)
    df_supplier = df_supplier.merge(Discounts, how = 'left', on = 'agent_id')
    df_supplier['Price'] = df_supplier['Price'] * df_supplier['Discount']

    #get table of edges which weights
    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(supplier_id_C_list)]
    df_edge = df_edge[df_edge['node_y_id'].isin(warehouse_id_list)]
    df_edge = df_edge[['node_x_id','distance']]

    #Append to original dataframe distance
    df_supplier = df_supplier.merge(df_edge, how = 'left', left_on = 'agent_id', right_on = 'node_x_id')

    #get table of edges which weights
    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(warehouse_id_list)]
    df_edge = df_edge[df_edge['node_y_id'].isin(assembly_id_list)]
    df_edge = df_edge[['node_x_id','distance']]
    df_edge.columns = ['node_x_id', 'distance2']
    distance_W_A = sum(df_edge['distance2'])

    #get table of edges which weights
    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(assembly_id_list)]
    df_edge = df_edge[df_edge['node_y_id'] == selected_retailer]
    df_edge = df_edge[['node_x_id','distance']]
    df_edge.columns = ['node_x_id', 'distance3']
    distance_A_R = sum(df_edge['distance3'])

    #add distance from supplier to warehouse, and from warehouse to assembly and from assembly to retailer
    df_supplier['distance'] = df_supplier['distance'] + distance_W_A + distance_W_A + distance_A_R

    #Increase distance by the number of deliveries (in this case 1)
    df_supplier['distance'] = df_supplier['distance'] * 1

    #Calculate total costs
    df_supplier['costs_of_raw_material'] = df_supplier['Price'].astype(float) * df_supplier['parts_demand']
    costs_of_raw_material = df_supplier['costs_of_raw_material'].sum()
    df_supplier['total_costs_of_transportation'] = long_route_cost
    total_costs_of_transportation = df_supplier['total_costs_of_transportation'].sum()

    total_costs_of_long_order = costs_of_raw_material + total_costs_of_transportation

    comparison = {'day': day,
    'selected_retailer': selected_retailer,
    'total_costs_of_short_order': total_costs_of_short_order,
    'total_costs_of_long_order': total_costs_of_long_order
    }

    return comparison

def get_order_size_from_retailer(selected_retailer, db, day):

    #Get estiamted demand from retail inventory of all agents
    df_retailer_inventory = db.table_to_df('retail_inventory')
    df_retailer_inventory = df_retailer_inventory[df_retailer_inventory['day'] >= day]

    #get inventory table of specific retailer
    df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]

    #Create a list of demand
    product_demand = df_active['estimated_order'].tolist()

    return product_demand

def get_stock_level_from_retailer(selected_retailer, db, day):

    #Get estiamted demand from retail inventory of all agents
    df_retailer_inventory = db.table_to_df('retail_inventory')
    df_retailer_inventory = df_retailer_inventory[df_retailer_inventory['day'] == day]

    #get inventory table of specific retailer
    df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]

    #Create a list of demand
    product_demand = df_active['stock_level'].tolist()

    return product_demand

def select_supplier_lead_time(db, day, selected_retailer):

    #In this case, the maximum lead time from china is 15 days
    #While the maximum lead time from local producer is 5 days
    #Thus comparison of 1 long order with 3 short orders are made

    ##################################################
    ########### Calculation of short order ###########
    ##################################################

    #retrieve supplier ID of retailer origin country
    supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)
    assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)

    df_supplier = agent_to_data_frame('Supplier', db)
    df_supplier = df_supplier[df_supplier['agent_id'].isin(supplier_id_R_list)]
    df_supplier = df_supplier[['agent_id']]

    #get table of edges with weight as duration
    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(supplier_id_R_list)]
    df_edge = df_edge[df_edge['node_y_id'].isin(assembly_id_list)]
    df_edge = df_edge[['node_x_id','duration']]

    #Append to original dataframe duration
    df_supplier = df_supplier.merge(df_edge, how = 'left', left_on = 'agent_id', right_on = 'node_x_id')

    #get production duration of assembly
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_id'].isin(assembly_id_list)]
    df_agents = df_agents[df_agents['attribute'] == 'Production_duration']
    production_duration = int(df_agents['value'])

    #Assign duration for travel between assembly and retail
    duration_A_R = 1

    #Calculate total duration per order
    df_supplier['duration'] = df_supplier['duration'] + production_duration + duration_A_R

    #Increase duration by the number of deliveries (in this case 3)
    df_supplier['duration'] = df_supplier['duration'] * 3

    #Calculate total duration
    total_short_order_duration = sum(df_supplier['duration'])
    
    ##################################################
    ########### Calculation of long order ###########
    ##################################################

    #retrieve supplier ID of retailer origin country
    supplier_id_C_list = retrieve_foreign_supplier_list(db, 'C')
    assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)
    warehouse_id_list = retrieve_relevant_warehouse_list(db, selected_retailer)

    df_supplier = agent_to_data_frame('Supplier', db)
    df_supplier = df_supplier[df_supplier['agent_id'].isin(supplier_id_C_list)]
    df_supplier = df_supplier[['agent_id']]

    #get table of edges with weight as duration
    df_edge = db.table_to_df('edge')
    df_edge = df_edge[df_edge['node_x_id'].isin(supplier_id_C_list)]
    df_edge = df_edge[df_edge['node_y_id'].isin(warehouse_id_list)]
    df_edge = df_edge[['node_x_id','duration']]

    #Append to original dataframe duration
    df_supplier = df_supplier.merge(df_edge, how = 'left', left_on = 'agent_id', right_on = 'node_x_id')

    #get operation duration of warehouse
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_id'].isin(warehouse_id_list)]
    df_agents = df_agents[df_agents['attribute'] == 'Operation_duration']
    warehouse_duration = int(df_agents['value'])

    #get production duration of assembly
    df_agents = db.table_to_df('agents')
    df_agents = df_agents[df_agents['agent_id'].isin(assembly_id_list)]
    df_agents = df_agents[df_agents['attribute'] == 'Production_duration']
    production_duration = int(df_agents['value'])

    #Assign duration for travel between assembly and retail
    duration_A_R = 1

    #Calculate total duration per order
    df_supplier['duration'] = df_supplier['duration'] + warehouse_duration + production_duration + duration_A_R

    #Increase duration by the number of deliveries (in this case 3)
    df_supplier['duration'] = df_supplier['duration'] * 3

    #Calculate total duration
    total_long_order_duration = sum(df_supplier['duration'])

    comparison = {'day': day,
    'selected_retailer': selected_retailer,
    'total_short_order_duration': total_short_order_duration,
    'total_long_order_duration': total_long_order_duration
    }

    return comparison

#select supplier priority based on previous history of on time deliveries
def select_supplier_on_time(db, day, selected_retailer):
    
    ##################################################
    ########### Calculation of short order ###########
    ##################################################

    supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)
    assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)

    all_short_routes = create_stop_id_short_order(db, day, supplier_id_R_list, assembly_id_list, selected_retailer)

    short_on_time_estimated_count = predict_on_time_status(db, supplier_id_R_list, all_short_routes, selected_retailer)

    ##################################################
    ########### Calculation of long order ###########
    ##################################################

    supplier_id_C_list = retrieve_foreign_supplier_list(db, 'C')
    assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)
    warehouse_id_list = retrieve_relevant_warehouse_list(db, selected_retailer)

    all_long_routes = create_stop_id_long_order(db, day, supplier_id_C_list, warehouse_id_list, assembly_id_list, selected_retailer)

    long_on_time_estimated_count = predict_on_time_status(db, supplier_id_C_list, all_long_routes, selected_retailer)

    comparison = {'day': day,
    'selected_retailer': selected_retailer,
    'short_on_time_estimated_count': short_on_time_estimated_count,
    'long_on_time_estimated_count': long_on_time_estimated_count
    }

    return comparison

#based on the sequence of supply chain, create a dataframe of all stop_id and planned day
#the output is used to estimate propability on time delivery
#The short version is from local suppliers without warehouse
def create_stop_id_short_order(db, day_begining, supplier_id_R_list, assembly_id_list, selected_retailer):

    max_estimated_day = calculate_max_lead_time(db)

    all_routes = pd.DataFrame()

    for route_no in range(1,4):
        for selected_supplier in supplier_id_R_list:
            route = [selected_supplier] + assembly_id_list + [selected_retailer]
            day_planned = day_begining * (max_estimated_day / 3) * route_no
            day_planned = int(day_planned)
            route = pd.DataFrame({'stop_id': route})
            route['Day_planned'] = day_planned
            route = pd.DataFrame(route)
            all_routes = all_routes.append(route,ignore_index=True)


    return all_routes

#based on the sequence of supply chain, create a dataframe of all stop_id and planned day
#the output is used to estimate propability on time delivery
#The long version is from off-shore suppliers with warehouse
def create_stop_id_long_order(db, day_begining, supplier_id_C_list, warehouse_id_list, assembly_id_list, selected_retailer):

    all_routes = pd.DataFrame()

    for selected_supplier in supplier_id_C_list:
        route = [selected_supplier] + warehouse_id_list + assembly_id_list + [selected_retailer]
        day_planned = day_begining
        route = pd.DataFrame({'stop_id': route})
        route['Day_planned'] = day_planned
        route = pd.DataFrame(route)
        all_routes = all_routes.append(route,ignore_index=True)

    return all_routes

#Generate product ID based on previous orders
def generate_unique_order_id_product(db):
    
    df_product_orders = db.table_to_df('product_orders')
    last_order_id = df_product_orders[['order_id']]
    if last_order_id.empty == True:
        order_id = 'FP_1'
    else:
        order_id = pd.DataFrame(last_order_id['order_id'].str.split('_',1).tolist(), columns = ['type','id'])
        order_id['id'] = order_id['id'].astype('int')
        order_id = int(order_id['id'].max())
        order_id = order_id + 1
        order_id = "FP_" + str(order_id)

    return order_id

#Generate order ID for raw material based on previous orders
def generate_unique_order_id_raw_material(db):

    df_raw_material_orders = db.table_to_df('raw_material_orders')
    last_order_id = df_raw_material_orders[['order_id']]

    if last_order_id.empty == True:
        order_id = 'RM_1'
    else:
        order_id = pd.DataFrame(last_order_id['order_id'].str.split('_',1).tolist(), columns = ['type','id'])
        order_id['id'] = order_id['id'].astype('int')
        order_id = int(order_id['id'].max())
        order_id = order_id + 1
        order_id = "RM_" + str(order_id)

    return order_id

#Generate order ID for raw material based on previous orders
def generate_unique_schedule_id(db):

    df_schedule_information = db.table_to_df('schedule_information')
    last_schedule_id = df_schedule_information[['schedule_id']]

    if last_schedule_id.empty == True:
        last_schedule_id = 'S_1'
    else:
        last_schedule_id = pd.DataFrame(last_schedule_id['schedule_id'].str.split('_',1).tolist(), columns = ['type','id'])
        last_schedule_id['id'] = last_schedule_id['id'].astype('int')
        last_schedule_id = int(last_schedule_id['id'].max())
        last_schedule_id = last_schedule_id + 1
        last_schedule_id = "S_" + str(last_schedule_id)

    return last_schedule_id

#Create a schedule and input to schedule information
#The input function is designed for short route i.e. without warehouse
def insert_short_route_to_schedule_information(db, day, route_no, selected_retailer, selected_supplier, assembly_id_list, order_id_raw_material, order_id_product, schedule_id):

    max_estimated_day = calculate_max_lead_time_for_short_route(db)
    
    if route_no == 1:
        day_begining = day
    else:
        day_begining = int(day + (max_estimated_day * route_no) - 1)

    #The begining of the route starts from the supplier
    route_1 = {'order_id': order_id_raw_material,
    'stop_id': selected_supplier,
    'Day_planned': day_begining,
    'Day_actual': day_begining,
    'Status': 'Order shipped',
    'On_time': 1,
    'Disrupted': 0}

    transit_duration_S_A = get_duration(db, selected_supplier, assembly_id_list)

    #the second point is the assembly facility
    route_2 = {'order_id': order_id_raw_material,
    'stop_id': assembly_id_list[0],
    'Day_planned': int(day_begining + transit_duration_S_A) - 1,
    'Day_actual': int(day_begining + transit_duration_S_A) - 1,
    'Status': 'Waiting production',
    'On_time': 1,
    'Disrupted': 0}

    product_order_test = db.select_where('SELECT order_id FROM schedule_information WHERE order_id = (%s)', (order_id_product, ))
    
    if len(product_order_test) == 0:

        #The third point, the retailer
        route_3 = {'order_id': order_id_product,
        'stop_id': selected_retailer,
        'Day_planned': int(day_begining + transit_duration_S_A) + 2,
        'Day_actual': int(day_begining + transit_duration_S_A) + 2,
        'Status': 'Waiting production',
        'On_time': 1,
        'Disrupted': 0}

        all_routes = pd.DataFrame([route_1, route_2, route_3])

    else:

        all_routes = pd.DataFrame([route_1, route_2])

    all_routes['schedule_id'] = schedule_id

    for _, row in all_routes.iterrows():
        db.insert_to_table('INSERT INTO schedule_information (Schedule_ID, Order_ID, Stop_ID, Day_planned, Day_actual, order_status, On_time, Disrupted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (row['schedule_id'], row['order_id'], row['stop_id'], row['Day_planned'], row['Day_actual'], row['Status'], row['On_time'], row['Disrupted']))

#Create a schedule and input to schedule information
#The input function is designed for long route i.e. with warehouse
def insert_long_route_to_schedule_information(db, day, selected_retailer, selected_supplier, warehouse_id_list, assembly_id_list, order_id_raw_material, order_id_product, schedule_id):

    day_begining = day

    #The begining of the route starts from the supplier
    route_1 = {'order_id': order_id_raw_material,
    'stop_id': selected_supplier,
    'Day_planned': day_begining,
    'Day_actual': day_begining,
    'Status': 'Order shipped',
    'On_time': 1,
    'Disrupted': 0}

    transit_duration_S_W = get_duration(db, selected_supplier, warehouse_id_list)
    transit_duration_W_A = get_duration(db, warehouse_id_list, assembly_id_list)

    #the second point does not display warehouse, however takes in to consideration the duration
    route_2 = {'order_id': order_id_raw_material,
    'stop_id': assembly_id_list[0],
    'Day_planned': int(day_begining + transit_duration_S_W + transit_duration_W_A + 1), #Production duration
    'Day_actual': int(day_begining + transit_duration_S_W + transit_duration_W_A + 1),
    'Status': 'Waiting production',
    'On_time': 1,
    'Disrupted': 0}

    product_order_test = db.select_where('SELECT order_id FROM schedule_information WHERE order_id = (%s)', (order_id_product, ))
    
    if len(product_order_test) == 0:

        #The third point, the retailer
        route_3 = {'order_id': order_id_product,
        'stop_id': selected_retailer,
        'Day_planned': int(day_begining + transit_duration_S_W + transit_duration_W_A + 2), #Production and transit duration
        'Day_actual':int( day_begining + transit_duration_S_W + transit_duration_W_A + 2),
        'Status': 'Waiting production',
        'On_time': 1,
        'Disrupted': 0}

        all_routes = pd.DataFrame([route_1, route_2, route_3])

    else:

        all_routes = pd.DataFrame([route_1, route_2])

    all_routes['schedule_id'] = schedule_id

    for _, row in all_routes.iterrows():
        db.insert_to_table('INSERT INTO schedule_information (Schedule_ID, Order_ID, Stop_ID, Day_planned, Day_actual, order_status, On_time, Disrupted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (row['schedule_id'], row['order_id'], row['stop_id'], row['Day_planned'], row['Day_actual'], row['Status'], row['On_time'], row['Disrupted']))

#Inserts the product order based on the short route
#In this case, 3 routes are created since the total demand is divided per 3 deliveries
def insert_short_route_product_order(db, day, route_no, selected_retailer, assembly_id_list, order_id_product, short_route_cost):

    estimated_demand = get_order_size_from_retailer(selected_retailer, db, day)

    total_stock_level = get_stock_level_from_retailer(selected_retailer, db, day - 1)
    if len(total_stock_level) != 0:
        total_stock_level = total_stock_level[0]
        stock_level = int(total_stock_level / len(estimated_demand))
        estimated_demand = [stock_level - x for x in estimated_demand]
        if total_stock_level > sum(estimated_demand):
            estimated_demand = [stock_level - x for x in estimated_demand]

    max_estimated_day = calculate_max_lead_time_for_short_route(db)
    
    if route_no == 1:
        current_demand = sum(estimated_demand[0:5])
        day_begining = day
    if route_no == 2:
        current_demand = sum(estimated_demand[5:10])
        day_begining = int(day + (max_estimated_day * route_no) - 1)
    if route_no == 3:
        current_demand = sum(estimated_demand[10:15])
        day_begining = int(day + (max_estimated_day * route_no) - 1)

    Cost_of_transportation = int(get_distance(db, assembly_id_list, selected_retailer) * short_route_cost)

    product_order = {'agent_id': selected_retailer,
    'day': int(day_begining),
    'order_id': order_id_product,
    'order_type': 'Finished products',
    'order_quantity': current_demand * 1.5,
    'order_status': "Waiting production",
    'current_order_from': assembly_id_list[0],
    'current_order_to': selected_retailer,
    'Cost_of_transportation': Cost_of_transportation
    }

    db.insert_to_table('INSERT INTO product_orders (Agent_ID, Day, Order_ID, Order_type, Order_quantity, Order_status, Current_order_from, Current_order_to, Cost_of_transportation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (product_order['agent_id'], product_order['day'], product_order['order_id'], product_order['order_type'], product_order['order_quantity'], product_order['order_status'], product_order['current_order_from'], product_order['current_order_to'], product_order['Cost_of_transportation']))

#inserts the product order based on the long route
#in this case, 1 route is generated since high quantity is ordered
def insert_long_route_product_order(db, day, selected_retailer, assembly_id_list, order_id_product, short_route_cost):

    estimated_demand = get_order_size_from_retailer(selected_retailer, db, day)
    current_demand = sum(estimated_demand)

    total_stock_level = get_stock_level_from_retailer(selected_retailer, db, day - 1)
    if len(total_stock_level) != 0:
        total_stock_level = total_stock_level[0]
        if total_stock_level > current_demand:
            current_demand = 0

    cost_of_transportation = get_distance(db, assembly_id_list, selected_retailer) * short_route_cost

    product_order = {'agent_id': selected_retailer,
    'day': day,
    'order_id': order_id_product,
    'order_type': 'Finished products',
    'order_quantity': current_demand,
    'order_status': 'Waiting production',
    'current_order_from': assembly_id_list[0],
    'current_order_to': selected_retailer,
    'cost_of_transportation': int(cost_of_transportation)}

    db.insert_to_table('INSERT INTO product_orders (Agent_ID, Day, Order_ID, Order_type, Order_quantity, Order_status, Current_order_from, Current_order_to, Cost_of_transportation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (product_order['agent_id'], product_order['day'], product_order['order_id'], product_order['order_type'], product_order['order_quantity'], product_order['order_status'], product_order['current_order_from'], product_order['current_order_to'], product_order['cost_of_transportation']))

#Inserts the raw materials order based on the short route
#In this case, 3 routes are created since the total demand is divided per 3 deliveries
def insert_short_route_raw_material_order(db, day, route_no, selected_retailer, assembly_id_list, selected_supplier, order_id_raw_material, short_route_cost):

    order_type = db.select_where('SELECT value FROM agents WHERE agent_id = (%s) AND attribute = (%s)', (selected_supplier, 'Type_of_raw_material'))
    order_type = order_type[0][0]

    required_parts = db.select_where('SELECT quantity FROM bill_of_material WHERE raw_material_id = (%s)', (order_type, ))
    required_parts = required_parts[0][0]

    estimated_demand = get_order_size_from_retailer(selected_retailer, db, day)

    total_stock_level = get_stock_level_from_retailer(selected_retailer, db, day - 1)
    if len(total_stock_level) != 0:
        total_stock_level = total_stock_level[0]
        if total_stock_level > sum(estimated_demand):
            estimated_demand = [0 for x in estimated_demand]

    max_estimated_day = calculate_max_lead_time_for_short_route(db)

    if route_no == 1:
        current_demand = sum(estimated_demand[0:5])
        day_begining = day
    if route_no == 2:
        current_demand = sum(estimated_demand[5:10])
        day_begining = int(day + (max_estimated_day * route_no) - 1)
    if route_no == 3:
        current_demand = sum(estimated_demand[10:15])
        day_begining = int(day + (max_estimated_day * route_no) - 1)

    current_demand = current_demand * 1.5
    current_demand = current_demand * required_parts

    df_raw_material_orders = db.table_to_df('raw_material_orders')
    df_active = df_raw_material_orders[df_raw_material_orders['current_order_from'] == selected_supplier]
    df_active = df_active.tail(5)
    consecetive_purchase = df_active['current_order_from'].tolist().count(selected_supplier)
    Discount = 1 - ((consecetive_purchase * 5) / 100)

    price_of_raw_materials = db.select_where('SELECT value FROM agents WHERE agent_id = (%s) AND Attribute = (%s)', (selected_supplier, 'Price'))
    price_of_raw_materials = int(float(price_of_raw_materials[0][0]))

    Cost_of_transportation = int(get_distance(db, selected_supplier, assembly_id_list) * short_route_cost)

    raw_material_order = {'agent_id': assembly_id_list[0],
    'day': int(day_begining),
    'order_id': order_id_raw_material,
    'order_type': order_type,
    'order_quantity': current_demand,
    'order_status': 'In transit',
    'current_order_from': selected_supplier,
    'current_order_to': assembly_id_list[0],
    'cost_of_raw_material': current_demand * price_of_raw_materials * Discount,
    'Cost_of_transportation': Cost_of_transportation 
    }

    db.insert_to_table('INSERT INTO raw_material_orders (Agent_ID, Day, Order_ID, Order_type, Order_quantity, Order_status, Current_order_from, Current_order_to, Price_per_unit, Costs_of_raw_materials, Cost_of_transportation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (raw_material_order['agent_id'], raw_material_order['day'], raw_material_order['order_id'], raw_material_order['order_type'], raw_material_order['order_quantity'], raw_material_order['order_status'], raw_material_order['current_order_from'], raw_material_order['current_order_to'], price_of_raw_materials, raw_material_order['cost_of_raw_material'], raw_material_order['Cost_of_transportation']))

#inserts the raw materials order based on the long route
#in this case, 1 route is generated since high quantity is ordered
def insert_long_route_raw_material_order(db, day, selected_retailer, warehouse_id_list, assembly_id_list, selected_supplier, order_id_raw_material, long_route_cost):

    order_type = db.select_where('SELECT value FROM agents WHERE agent_id = (%s) AND attribute = (%s)', (selected_supplier, 'Type_of_raw_material'))
    order_type = order_type[0][0]

    required_parts = db.select_where('SELECT quantity FROM bill_of_material WHERE raw_material_id = (%s)', (order_type, ))
    required_parts = required_parts[0][0]

    estimated_demand = get_order_size_from_retailer(selected_retailer, db, day)
    current_demand = sum(estimated_demand)

    total_stock_level = get_stock_level_from_retailer(selected_retailer, db, day - 1)
    if len(total_stock_level) != 0:
        total_stock_level = total_stock_level[0]
        if total_stock_level > current_demand:
            current_demand = 0

    current_demand = current_demand * required_parts

    df_raw_material_orders = db.table_to_df('raw_material_orders')
    df_active = df_raw_material_orders[df_raw_material_orders['current_order_from'] == selected_supplier]
    df_active = df_active.tail(5)
    consecetive_purchase = df_active['current_order_from'].tolist().count(selected_supplier)
    Discount = 1 - ((consecetive_purchase * 5) / 100)

    price_of_raw_materials = db.select_where('SELECT value FROM agents WHERE agent_id = (%s) AND attribute = (%s)', (selected_supplier, 'Price'))
    price_of_raw_materials = int(float(price_of_raw_materials[0][0]))

    Cost_of_transportation = long_route_cost

    raw_material_order = {'agent_id': assembly_id_list[0],
    'day': day,
    'order_id': order_id_raw_material,
    'order_type': order_type,
    'order_quantity': current_demand,
    'order_status': 'In transit',
    'current_order_from': selected_supplier,
    'current_order_to': assembly_id_list[0],
    'cost_of_raw_material': current_demand * price_of_raw_materials * Discount,
    'Cost_of_transportation': Cost_of_transportation 
    }

    db.insert_to_table('INSERT INTO raw_material_orders (Agent_ID, Day, Order_ID, Order_type, Order_quantity, Order_status, Current_order_from, Current_order_to, Price_per_unit, Costs_of_raw_materials, Cost_of_transportation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (raw_material_order['agent_id'], raw_material_order['day'], raw_material_order['order_id'], raw_material_order['order_type'], raw_material_order['order_quantity'], raw_material_order['order_status'], raw_material_order['current_order_from'], raw_material_order['current_order_to'], price_of_raw_materials, raw_material_order['cost_of_raw_material'], raw_material_order['Cost_of_transportation']))

#The function generates the schedule based on the optimization goal
def generate_data_for_schedule(day, db, short_route_cost, long_route_cost, Optimization_goal = 'Profit'):

    df_retailer  = agent_to_data_frame('Retailer', db)
    df_retailer_id_list = df_retailer['agent_id']

    for selected_retailer in df_retailer_id_list:

        supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)
        supplier_id_C_list = retrieve_foreign_supplier_list(db, 'C')
        assembly_id_list = retrieve_relevant_assembly_list(db, selected_retailer)
        warehouse_id_list = retrieve_relevant_warehouse_list(db, selected_retailer)

        if Optimization_goal == 'Profit':
            decision_dict = select_supplier_costs(db, day, selected_retailer, short_route_cost, long_route_cost)

            if decision_dict['total_costs_of_short_order'] <= decision_dict['total_costs_of_long_order']:

                supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)

                for route_no in range(1,4):

                    order_id_product = generate_unique_order_id_product(db)

                    schedule_id = generate_unique_schedule_id(db)

                    insert_short_route_product_order(db, day, route_no, selected_retailer, assembly_id_list, order_id_product, short_route_cost)

                    for selected_supplier in supplier_id_R_list:
                    
                        order_id_raw_material = generate_unique_order_id_raw_material(db)

                        insert_short_route_to_schedule_information(db, day, route_no, selected_retailer, selected_supplier, assembly_id_list, order_id_raw_material, order_id_product, schedule_id)

                        insert_short_route_raw_material_order(db, day, route_no, selected_retailer, assembly_id_list, selected_supplier, order_id_raw_material, short_route_cost)

            else:

                order_id_product = generate_unique_order_id_product(db)

                schedule_id = generate_unique_schedule_id(db)

                insert_long_route_product_order(db, day, selected_retailer, assembly_id_list, order_id_product, short_route_cost)

                for selected_supplier in supplier_id_C_list:

                    order_id_raw_material = generate_unique_order_id_raw_material(db)
                    
                    insert_long_route_to_schedule_information(db, day, selected_retailer, selected_supplier, warehouse_id_list, assembly_id_list, order_id_raw_material, order_id_product, schedule_id)

                    insert_long_route_raw_material_order(db, day, selected_retailer, warehouse_id_list, assembly_id_list, selected_supplier, order_id_raw_material, long_route_cost)


        if Optimization_goal == 'Lead-time':
            decision_dict =  select_supplier_lead_time(db, day, selected_retailer)

            if decision_dict['total_short_order_duration'] <= decision_dict['total_long_order_duration']:

                supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)

                for route_no in range(1,4):

                    order_id_product = generate_unique_order_id_product(db)

                    schedule_id = generate_unique_schedule_id(db)

                    insert_short_route_product_order(db, day, route_no, selected_retailer, assembly_id_list, order_id_product, short_route_cost)

                    for selected_supplier in supplier_id_R_list:
                    
                        order_id_raw_material = generate_unique_order_id_raw_material(db)

                        insert_short_route_to_schedule_information(db, day, route_no, selected_retailer, selected_supplier, assembly_id_list, order_id_raw_material, order_id_product, schedule_id)

                        insert_short_route_raw_material_order(db, day, route_no, selected_retailer, assembly_id_list, selected_supplier, order_id_raw_material, short_route_cost)

            else:
                
                order_id_product = generate_unique_order_id_product(db)

                schedule_id = generate_unique_schedule_id(db)

                insert_long_route_product_order(db, day, selected_retailer, assembly_id_list, order_id_product, short_route_cost)

                for selected_supplier in supplier_id_C_list:

                    order_id_raw_material = generate_unique_order_id_raw_material(db)
                    
                    insert_long_route_to_schedule_information(db, day, selected_retailer, selected_supplier, warehouse_id_list, assembly_id_list, order_id_raw_material, order_id_product, schedule_id)

                    insert_long_route_raw_material_order(db, day, selected_retailer, warehouse_id_list, assembly_id_list, selected_supplier, order_id_raw_material, long_route_cost)

        if Optimization_goal == 'On time delivery':
            decision_dict = select_supplier_on_time(db, day, selected_retailer)

            if decision_dict['short_on_time_estimated_count'] <= decision_dict['long_on_time_estimated_count']:

                supplier_id_R_list = retrieve_relevant_supplier_list(db, selected_retailer)

                for route_no in range(1,4):

                    order_id_product = generate_unique_order_id_product(db)

                    schedule_id = generate_unique_schedule_id(db)

                    insert_short_route_product_order(db, day, route_no, selected_retailer, assembly_id_list, order_id_product, short_route_cost)

                    for selected_supplier in supplier_id_R_list:
                    
                        order_id_raw_material = generate_unique_order_id_raw_material(db)

                        insert_short_route_to_schedule_information(db, day, route_no, selected_retailer, selected_supplier, assembly_id_list, order_id_raw_material, order_id_product, schedule_id)

                        insert_short_route_raw_material_order(db, day, route_no, selected_retailer, assembly_id_list, selected_supplier, order_id_raw_material, short_route_cost)

            else:
                
                order_id_product = generate_unique_order_id_product(db)

                schedule_id = generate_unique_schedule_id(db)

                insert_long_route_product_order(db, day, selected_retailer, assembly_id_list, order_id_product, short_route_cost)

                for selected_supplier in supplier_id_C_list:

                    order_id_raw_material = generate_unique_order_id_raw_material(db)
                    
                    insert_long_route_to_schedule_information(db, day, selected_retailer, selected_supplier, warehouse_id_list, assembly_id_list, order_id_raw_material, order_id_product, schedule_id)

                    insert_long_route_raw_material_order(db, day, selected_retailer, warehouse_id_list, assembly_id_list, selected_supplier, order_id_raw_material, long_route_cost)