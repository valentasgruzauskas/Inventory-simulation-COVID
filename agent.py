from mesa import Agent

import scipy.stats as stats
import random
import numpy as np
import pandas as pd

from Support_functions import generate_demand, simulate_disruption, calculate_max_lead_time, get_mean_indicator_by_day, get_mean_indicator_by_day_order, select_database_title

from Database_connector import MyDatabase

from Demand_forecasting import naive_estimation, naive_ML_estimation, DDMRP

#Defines the supplier agent
class Supplier_agent(Agent):

    def __init__(self, unique_id, Type_of_raw_material, Lead_time_stable, Price, Disruption_level, Lead_time_disrupted, Disruption_intensity, model, db):
        super().__init__(unique_id, model)
        
        #Defines the initial values of the supplier agent
        self.Type_of_raw_material = Type_of_raw_material
        self.Lead_time_stable = Lead_time_stable
        self.Price = Price
        self.Disruption_level = Disruption_level
        self.Lead_time_disrupted = Lead_time_disrupted
        self.Disruption_intensity = Disruption_intensity
        self.Disruption_updated = 0
        self.Disruption_updated_2 = 0
        self.db = db

        df_calendar = db.table_to_df('Calendar')
        self.day = df_calendar['day'].max()

    #Change lad time of supplier based on disruption level
    def set_lead_time(self, day, db):

        if self.Disruption_level == 0:

            self.Lead_time_disrupted = self.Lead_time_stable

            db.update_table_row('UPDATE agents SET value = (%s) WHERE agent_id = (%s) AND attribute = (%s)', (int(self.Lead_time_disrupted), self.unique_id, 'Lead_time'))

            self.Disruption_updated = 0
            self.Disruption_updated_2 = 0

        if self.Disruption_level == 1:

            if self.Disruption_updated == 0:

                #if the disruption level is increased, the lead-time of the current order is increased.
                #For isntance, this assumes that some of the employees got sick.
                order_id = db.select_where('SELECT order_id FROM schedule_information WHERE stop_id = (%s) AND order_status = (%s) AND On_time = (%s) AND Disrupted = (%s)', (self.unique_id, 'Order shipped', 1, 0))
                order_id = [item for sublist in order_id for item in sublist]

                if len(order_id) != 0:

                    self.Lead_time_disrupted = int(self.Lead_time_stable * 1.5)

                    additional_days = int(self.Lead_time_disrupted - self.Lead_time_stable)

                    for selected_order_id in order_id:
                        day_actual = db.select_where('SELECT day_actual FROM schedule_information WHERE order_id = (%s) AND order_status = (%s)', (selected_order_id, 'Waiting production'))
                        day_actual = day_actual[0][0]
                        adjusted_days = additional_days + day_actual

                        db.update_table_row('UPDATE schedule_information SET day_actual = (%s) WHERE order_id = (%s) AND order_status = (%s)', (adjusted_days, selected_order_id, 'Waiting production'))
                        db.update_table_row('UPDATE schedule_information SET On_time = (%s) WHERE order_id = (%s) AND order_status = (%s)', (0, selected_order_id, 'Waiting production'))
                        db.update_table_row('UPDATE schedule_information SET Disrupted = (%s) WHERE order_id = (%s) AND order_status = (%s)', (1, selected_order_id, 'Waiting production'))

                db.update_table_row('UPDATE agents SET value = (%s) WHERE agent_id = (%s) AND attribute = (%s)', (int(self.Lead_time_disrupted), self.unique_id, 'Lead_time'))

                self.Disruption_updated = 1

        if self.Disruption_level == 2:

            if self.Disruption_updated_2 == 0:

                #if the disruption level is increased, the lead-time of the current order is increased.
                #For isntance, this assumes that some of the employees got sick.
                order_id = db.select_where('SELECT order_id FROM schedule_information WHERE stop_id = (%s) AND order_status = (%s) AND On_time = (%s) AND Disrupted = (%s)', (self.unique_id, 'Order shipped', 0, 1))
                order_id = [item for sublist in order_id for item in sublist]

                if len(order_id) != 0:

                    self.Lead_time_disrupted = int(self.Lead_time_stable * 2)

                    additional_days = int(self.Lead_time_disrupted - self.Lead_time_stable)

                    for selected_order_id in order_id:
                        day_actual = db.select_where('SELECT day_actual FROM schedule_information WHERE order_id = (%s) AND order_status = (%s)', (selected_order_id, 'Waiting production'))
                        day_actual = day_actual[0][0]
                        adjusted_days = additional_days + day_actual

                        db.update_table_row('UPDATE schedule_information SET day_actual = (%s) WHERE order_id = (%s) AND order_status = (%s)', (adjusted_days, selected_order_id, 'Waiting production'))
                        db.update_table_row('UPDATE schedule_information SET On_time = (%s) WHERE order_id = (%s) AND order_status = (%s)', (0, selected_order_id, 'Waiting production'))
                        db.update_table_row('UPDATE schedule_information SET Disrupted = (%s) WHERE order_id = (%s) AND order_status = (%s)', (2, selected_order_id, 'Waiting production'))

                db.update_table_row('UPDATE agents SET value = (%s) WHERE agent_id = (%s) AND attribute = (%s)', (int(self.Lead_time_disrupted), self.unique_id, 'Lead_time'))

                self.Disruption_updated_2 = 1

        db.insert_to_table('INSERT INTO lead_time_history (Day, supplier_Id, Lead_time) VALUES (%s, %s, %s)', (day, self.unique_id, self.Lead_time_disrupted))

    # the steps that the supplier agent must take every iteration
    def step(self):
        df_calendar = self.db.table_to_df('Calendar')
        self.day = df_calendar['day'].max()
        self.Disruption_level = simulate_disruption(self.Disruption_level, self.Disruption_intensity)
        self.set_lead_time(self.day, self.db)

#Defines the warehouse agent
class Warehouse_agent(Agent):

    def __init__(self, unique_id, Operation_duration, model):
        super().__init__(unique_id, model)
        
        #initializes the initial parameters of the agent
        self.Operation_duration = Operation_duration
    
    def step(self):
        pass

#defines tha assembly agent
class Assembly_agent(Agent):

    def __init__(self, unique_id, Production_duration, model, db):
        super().__init__(unique_id, model)
       
        #initializes te initial parameters of the agent
        self.unique_id = unique_id
        self.Production_duration = Production_duration
        self.db = db

        df_calendar = db.table_to_df('Calendar')
        self.day = df_calendar['day'].max()

        self.insert_part_names_to_inventory(self.day, self.db)

    #inserts empty lines for current day for all raw material parts, and finished products to the inventory table
    def insert_part_names_to_inventory(self, day, db):
        
        BOM = db.table_to_df('bill_of_material')
        #loop or dublicate for multiple products
        BOM = BOM[BOM['product_id'] == 1]
        material_id_list = BOM['raw_material_id'].tolist()
        material_id_list.append('Finished products')
        df_temp = pd.DataFrame(material_id_list)
        df_temp.columns = ['Stock_level_type']
        df_temp['agent_id'] = self.unique_id
        df_temp['day'] = day
        df_temp['stock_level'] = 0
        df_temp['Costs_of_production'] = 0

        for _, raw in df_temp.iterrows():
            db.insert_to_table('INSERT INTO production_inventory (Agent_ID, Day, Stock_level_type, Stock_level, Costs_of_production) VALUES (%s, %s, %s, %s, %s)', (raw['agent_id'], raw['day'], raw['Stock_level_type'], raw['stock_level'], raw['Costs_of_production']))

    def update_production_inventory_levels(self, day, db):

        BOM = db.table_to_df('bill_of_material')
        #loop or dublicate for multiple products
        BOM = BOM[BOM['product_id'] == 1]
        material_id_list = BOM['raw_material_id'].tolist()
        material_id_list.append('Finished products')

        for selected_material_id in material_id_list:
            
            previous_stock_level = db.select_where('SELECT Stock_level FROM production_inventory WHERE day = (%s) AND stock_level_type = (%s) AND agent_id = (%s)', (day - 1, selected_material_id, self.unique_id))
            previous_stock_level = previous_stock_level[0][0]  

            db.update_table_row('UPDATE production_inventory SET stock_level = (%s) WHERE agent_id = (%s) and day = (%s) and stock_level_type = (%s)', (previous_stock_level, self.unique_id, day, selected_material_id))

    #Checks the current raw material stock level and assembles products
    def produce_products(self, day, db, costs_per_unit):
        
        #get stock level of all raw materials
        stock_levels = db.table_to_df('production_inventory')
        stock_levels = stock_levels[(stock_levels['agent_id'] == self.unique_id) & (stock_levels['day'] == day)]
        stock_levels = stock_levels[['stock_level_type', 'stock_level']]

        BOM = db.table_to_df('bill_of_material')
        #loop or dublicate for multiple products
        BOM = BOM[BOM['product_id'] == 1]
        BOM = BOM[['raw_material_id','quantity']]

        #Check the maximum number of products to produce
        stock_levels = stock_levels.merge(BOM, left_on='stock_level_type', right_on='raw_material_id')
        stock_levels['products_max'] = stock_levels['stock_level'] / stock_levels['quantity']
        products_max = int(stock_levels['products_max'].min())

        if products_max != 0:
            stock_levels['total_raw_material'] = stock_levels['quantity'] * products_max
        else:
            stock_levels['total_raw_material'] = 0

        stock_levels['new_Stock_level'] = stock_levels['stock_level'] - stock_levels['total_raw_material']  

        #updates the raw material stock level in the ivnentory table
        for _, raw in stock_levels.iterrows():
            db.update_table_row('UPDATE production_inventory SET stock_level = (%s) WHERE agent_id = (%s) and day = (%s) and stock_level_type = (%s)', (raw['new_Stock_level'], self.unique_id, day, raw['raw_material_id']))

        #updates the stock level of finished products, and costs of assembly
        db.update_table_row('UPDATE production_inventory SET Stock_level = Stock_level + (%s) WHERE agent_id = (%s) and day = (%s) and stock_level_type = (%s)', (products_max, self.unique_id, day, 'Finished products'))

        if products_max != 0:
            db.update_table_row('UPDATE production_inventory SET Costs_of_production = (%s) WHERE agent_id = (%s) and day = (%s) and stock_level_type = (%s)', (products_max * costs_per_unit, self.unique_id, day, 'Finished products'))

    #receives the order from the supplier, if it has arrived at the assembly facility
    def receive_order_from_supplier(self, day, db):
        
        #Receives the order_ID for orders that are in trasnit for a specific assembly agent
        order_id = db.select_where('SELECT order_id FROM Raw_material_orders WHERE current_order_to = (%s) AND Order_status = (%s)', (self.unique_id, "In transit"))
        order_id = [item for sublist in order_id for item in sublist]

        #obtains the schedule for the orders that are in transit
        compare_arrival = db.table_to_df('schedule_information')
        compare_arrival = compare_arrival[(compare_arrival['stop_id'] == self.unique_id) & (compare_arrival['order_id'].isin(order_id))]

        #if the actual day of the schedule matches the current day change the status of the order to Arrived
        for _, raw in compare_arrival.iterrows(): 
            if raw['day_actual'] == day:
                db.update_table_row('UPDATE schedule_information SET Order_status = (%s) WHERE order_id = (%s)', ("Arrived", raw['order_id']))
                db.update_table_row('UPDATE Raw_material_orders SET Order_status = (%s) WHERE order_id = (%s)', ("Arrived", raw['order_id']))

                #select all the orders from the raw material table, which have arrived
                raw_material_orders = db.table_to_df('Raw_material_orders')
                raw_material_orders = raw_material_orders[raw_material_orders['order_id'] == raw['order_id']]

                #check if the the obtained order dataframe is not empty
                if raw_material_orders.empty == False:
                    
                    raw_material_orders = raw_material_orders[['order_id', 'order_type','order_quantity']]

                    #loop trough all the obtained orders and update the stock level in a partucular day, for assembly agent and specific type of raw material.
                    for _, raw in raw_material_orders.iterrows():
                        current_stock_level = db.select_where('SELECT Stock_level FROM production_inventory WHERE day = (%s) AND stock_level_type = (%s) AND agent_id = (%s)', (day, raw['order_type'], self.unique_id))
                        current_stock_level = current_stock_level[0][0]
                        new_stock_level = current_stock_level + raw['order_quantity']
                        db.update_table_row('UPDATE production_inventory SET Stock_level = (%s) WHERE agent_id = (%s) AND day = (%s) AND stock_level_type = (%s)', (new_stock_level, self.unique_id, day, raw['order_type']))
                        db.update_table_row('UPDATE Raw_material_orders SET order_status = (%s) WHERE agent_id = (%s) AND order_id = (%s) AND order_type = (%s)', ('Delivered', self.unique_id, raw['order_id'], raw['order_type']))
                        db.update_table_row('UPDATE schedule_information SET order_status = (%s) WHERE stop_id = (%s) AND Day_actual = (%s)', ("Delivered", self.unique_id, day))

                        df_agents = db.table_to_df('agents')
                        df_agents = df_agents[df_agents['value'] == raw['order_type']]
                        df_agents = df_agents['agent_id'].tolist()

                        db.update_table_row('UPDATE schedule_information SET order_status = (%s) WHERE stop_id = (%s) AND order_id = (%s)', ("Delivered", df_agents[0], raw['order_id']))
                        db.update_table_row('UPDATE schedule_information SET order_status = (%s) WHERE stop_id = (%s) AND order_id = (%s)', ("Delivered", df_agents[1], raw['order_id']))
    
    #Check the stock level of finished products and send them to the retailer.           
    def send_finished_products(self, day, db): 

        df_product_orders = db.table_to_df('product_orders')
        df_product_orders = df_product_orders[(df_product_orders['current_order_from'] == self.unique_id) & (df_product_orders['order_status'] == 'Waiting production')]

        df_schedule_information = db.table_to_df('schedule_information')

        for _, raw in df_product_orders.iterrows():

            #Receive the current level of the finished products
            current_finished_products = db.select_where('SELECT Stock_level FROM production_inventory WHERE agent_id = (%s) AND day = (%s) AND stock_level_type = (%s)', (self.unique_id, day, 'Finished products'))
            current_finished_products = current_finished_products[0][0]

            #check the ordered quantity of products needed
            orders_waiting = raw['order_quantity']

            #Check if the current stock level is sufficient in the assembly facility
            if current_finished_products >= orders_waiting:

                #Decrease the stock level of finished products in the inventory table
                new_stock_level = current_finished_products - orders_waiting
                db.update_table_row('UPDATE production_inventory SET Stock_level = (%s) WHERE agent_id = (%s) and day = (%s) and Stock_level_type = (%s)', (new_stock_level, self.unique_id, day, 'Finished products'))

                #Update the status of the order list, that it is in transit.
                db.update_table_row('UPDATE product_orders SET order_status = (%s) WHERE Order_ID = (%s)', ('In transit', raw['order_id']))

                #update schedule information from Waiting production to "In transit"
                db.update_table_row('UPDATE schedule_information SET order_status = (%s) WHERE order_id = (%s)', ('In transit', raw['order_id']))

                compare_arrival = df_schedule_information[(df_schedule_information['order_id'] == raw['order_id'])]

                #check if the order is late
                if int(compare_arrival['day_actual']) <= day:
                    db.update_table_row('UPDATE schedule_information SET on_time = (%s) WHERE order_id = (%s)', (0, raw['order_id']))
                    db.update_table_row('UPDATE schedule_information SET day_actual = (%s) WHERE order_id = (%s)', (day + 1, raw['order_id']))

    # the steps that the assembly agent must take every iteration
    def step(self):
        df_calendar = self.db.table_to_df('Calendar')
        self.day = df_calendar['day'].max()

        if self.day != 0:
            self.insert_part_names_to_inventory(self.day, self.db)
            self.update_production_inventory_levels(self.day, self.db)

        self.receive_order_from_supplier(self.day, self.db)
        self.produce_products(self.day, self.db, costs_per_unit = 240)
        self.send_finished_products(self.day, self.db)

#define the retailer agent
class Retailer_agent(Agent):

    def __init__(self, unique_id, Cost_of_storage, Disruption_level, Stock_level, Disruption_intensity, model, db):
        super().__init__(unique_id, model)
        
        #initialize the initial parameters for the retail agent
        self.Cost_of_storage = Cost_of_storage
        self.Disruption_level = Disruption_level
        self.Disruption_intensity = Disruption_intensity
        self.Stock_level = Stock_level
        self.db = db

        df_calendar = db.table_to_df('Calendar')
        self.day = df_calendar['day'].max()

        self.insert_empty_line_to_inventory_table(self.day, self.db)
        order_size = self.receive_order_from_customer()
        self.db.update_table_row('UPDATE retail_inventory SET order_size = (%s) WHERE agent_id = (%s) and day = (%s)', (order_size, self.unique_id, self.day))

    #insert the new line for every day in the retail inventory table
    def insert_empty_line_to_inventory_table(self, day, db):
        
        db.insert_to_table('INSERT INTO retail_inventory (agent_id, day, stock_level, order_size, estimated_order, backlog, units_sold, storage_costs, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (self.unique_id, day, self.Stock_level, 0, 0, 0, 0, int(self.Cost_of_storage * self.Stock_level), 0))

    #Generate demand and disruption to the demand
    def receive_order_from_customer(self):
        
        #Generate order size
        order_size = generate_demand(120, 36)

        #Check if the retailer is in a disruptive state and decline order size acordingly
        if self.Disruption_level == 1:
            order_size = 0.5 * order_size
        if self.Disruption_level == 2:
            order_size = 0.5 * order_size

        return order_size

    #Deterine how much to sell
    def sell_products(self, order_size):
        
        #If stock level is higher than order size, sell maximum, otherwise sell whats possible
        if self.Stock_level > order_size:
            products_to_sell = order_size
        else:
            products_to_sell = self.Stock_level

        return products_to_sell

    #Update the inventory table with the quantity of produdcts soled
    def update_stock_level(self, day, db, products_to_sell, price_per_unit):
        
        #Update the current stock level of the retail agents
        self.Stock_level = self.Stock_level - products_to_sell
        #update the number of untis sold and revenue in the inventory table
        db.update_table_row('UPDATE retail_inventory SET Units_sold = (%s) WHERE agent_id = (%s) and Day = (%s)', (products_to_sell, self.unique_id, day))
        db.update_table_row('UPDATE retail_inventory SET Revenue = (%s) WHERE agent_id = (%s) and Day = (%s)', (products_to_sell * price_per_unit, self.unique_id, day))

        #Obtain the orders, which arrived at the retailer
        order_id = db.select_where('SELECT order_id FROM schedule_information WHERE stop_id = (%s) AND Order_status = (%s) AND Day_actual = (%s)', (self.unique_id, "In transit", day))
        order_id = [item for sublist in order_id for item in sublist]

        #if the order list is not empty
        if len(order_id) != 0:

            #Obtain the schedule list for the specific order
            compare_arrival = db.table_to_df('schedule_information')
            compare_arrival = compare_arrival[(compare_arrival['stop_id'] == self.unique_id) & (compare_arrival['order_id'].isin(order_id))]

            #if the orders day actual mathces with the current day, update the status to Arrived
            for _, raw in compare_arrival.iterrows(): 
                if raw['day_actual'] == day:
                    db.update_table_row('UPDATE schedule_information SET Order_status = (%s) WHERE order_id = (%s)', ("Arrived", raw['order_id']))
                    db.update_table_row('UPDATE product_orders SET Order_status = (%s) WHERE order_id = (%s)', ("Arrived", raw['order_id']))

                    #obtain the quantity of the incoming order
                    incoming_order = db.select_where('SELECT order_quantity FROM product_orders WHERE order_id = (%s)', (raw['order_id'], ))
                    incoming_order = incoming_order[0][0]

                    #Update the stock level of the retail inventory
                    self.Stock_level = int(self.Stock_level + incoming_order)
                    db.update_table_row('UPDATE retail_inventory SET Stock_level = (%s) WHERE agent_id = (%s) and Day = (%s)', (self.Stock_level, self.unique_id, day))

                    #update the costs of storage in the retail inventory
                    cost_of_storage_per_unit = db.select_where('SELECT value FROM agents WHERE agent_id = (%s) AND attribute = (%s)', (self.unique_id, 'Cost_of_storage'))
                    cost_of_storage_per_unit = int(cost_of_storage_per_unit[0][0])
                    Storage_costs = self.Stock_level * cost_of_storage_per_unit
                    db.update_table_row('UPDATE retail_inventory SET Storage_costs = (%s) WHERE agent_id = (%s) and Day = (%s)', (Storage_costs, self.unique_id, day))

                    #update the status of the order, that it is delivered
                    db.update_table_row('UPDATE product_orders SET order_status = (%s) WHERE agent_id = (%s) and order_id = (%s)', ('Delivered', self.unique_id, raw['order_id']))
                    db.update_table_row('UPDATE schedule_information SET order_status = (%s) WHERE stop_id = (%s) and day_actual = (%s)', ('Delivered', self.unique_id, day))

                    #Compare the the planned day, with the actual day of arrival
                    comparison_days = db.table_to_df('schedule_information')
                    comparison_days = comparison_days[comparison_days['stop_id'] == self.unique_id]
                    comparison_days = comparison_days[comparison_days['order_status'] == 'Delivered']
                    comparison_days = comparison_days[comparison_days['order_id'] == raw['order_id']]
                    comparison_days = int(comparison_days['day_actual']) - int(comparison_days['day_planned'])

                    #if the planned and actual day is not the same, set no (value: 0) to the on time variable.
                    if comparison_days != 0:
                        db.update_table_row('UPDATE schedule_information SET On_time = (%s) WHERE order_id = (%s)', (0, raw['order_id']))

    # the steps that the retail agent must take every iteration
    def step(self):

        df_calendar = self.db.table_to_df('Calendar')
        self.day = df_calendar['day'].max()
        
        self.Disruption_level = simulate_disruption(self.Disruption_level, self.Disruption_intensity)
        order_size = self.receive_order_from_customer()
        products_to_sell = self.sell_products(order_size)
        self.update_stock_level(self.day, self.db, products_to_sell, price_per_unit = 1200)

        self.db.update_table_row('UPDATE retail_inventory SET stock_level = (%s) WHERE agent_id = (%s) and day = (%s)', (self.Stock_level, self.unique_id, self.day))

        units_sold = self.db.select_where('SELECT units_sold FROM retail_inventory WHERE agent_id = (%s) AND day = (%s)', (self.unique_id, self.day))
        units_sold = units_sold[0][0]
        backlog = order_size - units_sold
        self.db.update_table_row('UPDATE retail_inventory SET backlog = (%s) WHERE agent_id = (%s) and Day = (%s)', (backlog, self.unique_id, self.day))

        self.db.update_table_row('UPDATE retail_inventory SET order_size = (%s) WHERE agent_id = (%s) and day = (%s)', (order_size, self.unique_id, self.day))
        cost_of_storage_per_unit = self.db.select_where('SELECT value FROM agents WHERE agent_id = (%s) AND attribute = (%s)', (self.unique_id, 'Cost_of_storage'))
        cost_of_storage_per_unit = int(cost_of_storage_per_unit[0][0])
        Storage_costs = self.Stock_level * cost_of_storage_per_unit
        self.db.update_table_row('UPDATE retail_inventory SET Storage_costs = (%s) WHERE agent_id = (%s) and Day = (%s)', (Storage_costs, self.unique_id, self.day))

