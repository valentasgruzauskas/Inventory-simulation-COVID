from mesa import Model
from mesa.time import BaseScheduler
from mesa.datacollection import DataCollector
import networkx as nx

from Database_connector import MyDatabase
from Create_database import setup_tables

from mesa.space import NetworkGrid

from Support_functions import agent_to_data_frame, to_report_KPI, to_report_LD, select_database_title, get_mean_indicator_by_day, get_mean_indicator_by_day_order, calculate_max_lead_time, calculate_KPI
from agent import Warehouse_agent, Supplier_agent, Assembly_agent, Retailer_agent

from Demand_forecasting import naive_estimation, naive_ML_estimation, DDMRP, make_orders

import numpy as np
import random

class InventorySimulation(Model):

    #Initialize model
    def __init__(self, Inventory_strategy, Optimization_goal, Disruption_intensity):

        self._seed = 6
        random.seed(6)
        np.random.seed(seed = 6)

        #define server name, based on scenario
        database_title = 'siminv' #select_database_title(Inventory_strategy, Optimization_goal)

        db = MyDatabase()
        db.create_con(database_title = database_title, user="postgres", port = 5432, password = "admin", host = 'localhost')

        setup_tables(db)

        df_edge = db.table_to_df('edge')
        self.G = nx.from_pandas_edgelist(df_edge, 'node_y_id', 'node_x_id', ['distance', 'duration'])
        self.schedule = BaseScheduler(self)
        self.grid = NetworkGrid(self.G)
        self.day = 0
        self.order_to_make = 0
        self.Inventory_strategy = Inventory_strategy
        self.Optimization_goal = Optimization_goal
        self.db = db
        self.short_route_cost = 1.58
        self.long_route_cost = 9000
        self.price_per_unit = 1200
        self.max_iters = 365 * 2
        db.insert_to_table('INSERT INTO Calendar (Day) VALUES (%s)', (self.day, ))

        #Create dataframes of agent information
        df_warehouse = agent_to_data_frame('Warehouse', db)
        df_supplier  = agent_to_data_frame('Supplier', db)
        df_retailer  = agent_to_data_frame('Retailer', db)
        df_assembly  = agent_to_data_frame('Assembly plant', db)

        #Data reporter for graphs
        self.datacollector = DataCollector(
            model_reporters={
                "Total orders": lambda m: to_report_KPI(self, db, self.day, 'Total_orders'),
                "Total inventory": lambda m: to_report_KPI(self, db,  self.day, 'Total_inventory'),
                "Backlog": lambda m: to_report_KPI(self, db,  self.day, 'Backlog'),
                "Order fulfilment": lambda m: to_report_KPI(self, db,  self.day, 'Order_fulfilment'),
                "Revenue": lambda m: to_report_KPI(self, db,  self.day, 'Revenue'),
                "Cost ordersof production": lambda m: to_report_KPI(self, db,  self.day, 'Cost_of_production'),
                "Costs of raw material": lambda m: to_report_KPI(self, db,  self.day, 'Costs_of_raw_material'),
                "Cost of delivery": lambda m: to_report_KPI(self, db,  self.day, 'Cost_of_delivery'),
                "Profit": lambda m: to_report_KPI(self, db,  self.day, 'Profit'),
                "Lead time short route": lambda m: to_report_LD(self, db,  self.day, 'short route'),
                "Lead time long route": lambda m: to_report_LD(self, db,  self.day, 'long route')
            }
        )

        ####### Create agents ######

        ### Retailer
        #Intial stock level is set to 25000, because the mean demand of the normal distribution is set to 120, and the maximum lead time is 15 days.
        #for valid result comparison it is recomended to remove first x ticks of the simulation.
        for _, row in df_retailer.iterrows():
            agents_r = Retailer_agent(row['agent_id'], int(float(row['Cost_of_storage'])), 0, 2500, Disruption_intensity, model = self, db = db)
            self.schedule.add(agents_r)
            self.grid.place_agent(agents_r, row['agent_id'])

        ### Assembly
        for _, row in df_assembly.iterrows():
            agents_a = Assembly_agent(row['agent_id'], int(float(row['Production_duration'])), model = self, db = db)
            self.schedule.add(agents_a)
            self.grid.place_agent(agents_a, row['agent_id'])

        #### Warehouse
        for _, row in df_warehouse.iterrows():
            agents_wh = Warehouse_agent(row['agent_id'], int(float(row['Operation_duration'])), self)
            self.schedule.add(agents_wh)
            self.grid.place_agent(agents_wh, row['agent_id'])

        ### Supplier
        for _, row in df_supplier.iterrows():
            agents_s = Supplier_agent(row['agent_id'], row['Type_of_raw_material'], int(float(row['Lead_time'])), int(float(row['Price'])), 0, int(float(row['Lead_time'])), Disruption_intensity, model = self, db = db)
            self.schedule.add(agents_s)
            self.grid.place_agent(agents_s, row['agent_id'])
        
        self.running = True

    def step(self):

        if self.day != 0 :
            self.db.insert_to_table('INSERT INTO Calendar (Day) VALUES (%s)', (self.day, ))

        if self.order_to_make == self.day:
            make_orders(self, self.day, self.db, self.short_route_cost, self.long_route_cost, self.Inventory_strategy, self.Optimization_goal)

        self.schedule.step()
        self.datacollector.collect(self)

        calculate_KPI(self, self.day, self.db, self.price_per_unit)

        self.day += 1

        if self.day > self.max_iters:
            self.running = False



