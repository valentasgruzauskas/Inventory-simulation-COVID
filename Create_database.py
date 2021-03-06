from Database_connector import MyDatabase

def setup_tables(db):

    #Create tables and import data
    db.query('DROP TABLE IF EXISTS Agents;')
    db.query('CREATE TABLE Agents (Agent_ID CHARACTER VARYING (255), Agent_name CHARACTER VARYING (255), Attribute CHARACTER VARYING (255), Value CHARACTER VARYING (255));')
    with open('Input/Agents.csv') as file:
        next(file)
        db.csv_to_table(file, "Agents", ",")

    db.query('DROP TABLE IF EXISTS Bill_of_material;')
    db.query('CREATE TABLE Bill_of_material (Raw_material_ID CHARACTER VARYING (255), Raw_material_type CHARACTER VARYING (255), Level INTEGER, Quantity INTEGER, product_ID INTEGER);')
    with open('Input/Bill of materials.csv') as file:
        next(file)
        db.csv_to_table(file, "Bill_of_material", ",")

    db.query('DROP TABLE IF EXISTS Products;')
    db.query('CREATE TABLE Products (Product_ID INTEGER, Product_name CHARACTER VARYING (255), Assembly_costs INTEGER, Price INTEGER);')
    with open('Input/Products.csv') as file:
        next(file)
        db.csv_to_table(file, "Products", ",")

    db.query('DROP TABLE IF EXISTS Vertex;')
    db.query('CREATE TABLE Vertex (Agent_ID CHARACTER VARYING (255));')
    with open('Input/Vertex.csv') as file:
        next(file)
        db.csv_to_table(file, "Vertex", ",")

    db.query('DROP TABLE IF EXISTS Edge;')
    db.query('CREATE TABLE Edge (Node_X_ID CHARACTER VARYING (255), Node_Y_ID CHARACTER VARYING (255), Distance DOUBLE PRECISION, Duration INTEGER);')
    with open('Input/Edge.csv') as file:
        next(file)
        db.csv_to_table(file, "Edge", ",")

    db.commit()

    #Create empty tables for simulation data storage
    db.query('DROP TABLE IF EXISTS Retail_inventory;')
    db.query('CREATE TABLE Retail_inventory (ID serial PRIMARY KEY, Agent_ID CHARACTER VARYING (255), Day INTEGER, Stock_level INTEGER, Order_size INTEGER, Estimated_order INTEGER, backlog INTEGER, Units_sold INTEGER, Storage_costs INTEGER, Revenue INTEGER);')

    db.query('DROP TABLE IF EXISTS schedule_information;')
    db.query('CREATE TABLE schedule_information (ID serial PRIMARY KEY, Schedule_ID CHARACTER VARYING (255), Order_ID CHARACTER VARYING (255), Stop_ID CHARACTER VARYING (255), Day_planned INTEGER, Day_actual INTEGER, Order_status CHARACTER VARYING (255), On_time INTEGER, Disrupted INTEGER);')

    db.query('DROP TABLE IF EXISTS production_inventory;')
    db.query('CREATE TABLE production_inventory (ID serial PRIMARY KEY, Agent_ID CHARACTER VARYING (255), Day INTEGER, Stock_level_type CHARACTER VARYING (255), Stock_level INTEGER, Costs_of_production INTEGER);')
    
    db.query('DROP TABLE IF EXISTS product_orders;')
    db.query('CREATE TABLE product_orders (ID serial PRIMARY KEY, Agent_ID CHARACTER VARYING (255), Day INTEGER, Order_ID CHARACTER VARYING (255), Order_type CHARACTER VARYING (255), Order_quantity INTEGER, Order_status CHARACTER VARYING (255), Current_order_from CHARACTER VARYING (255), Current_order_to CHARACTER VARYING (255), Cost_of_transportation INTEGER);')
    
    db.query('DROP TABLE IF EXISTS raw_material_orders')
    db.query('CREATE TABLE raw_material_orders (ID serial PRIMARY KEY, Agent_ID CHARACTER VARYING (255), Day INTEGER, Order_ID CHARACTER VARYING (255), Order_type CHARACTER VARYING (255), Order_quantity INTEGER, Order_status CHARACTER VARYING (255), Current_order_from CHARACTER VARYING (255), Current_order_to CHARACTER VARYING (255), Price_per_unit INTEGER, Costs_of_raw_materials INTEGER, Cost_of_transportation INTEGER);')

    db.query('DROP TABLE IF EXISTS KPI_table;')
    db.query('CREATE TABLE KPI_table (ID serial PRIMARY KEY, Day INTEGER, Total_orders INTEGER, Total_inventory INTEGER, Backlog INTEGER, Order_fulfilment DOUBLE PRECISION, Revenue INTEGER, Cost_of_production INTEGER, Costs_of_raw_material INTEGER, Cost_of_delivery INTEGER, costs_of_storage INTEGER, Profit INTEGER);')

    db.query('DROP TABLE IF EXISTS Calendar;')
    db.query('CREATE TABLE Calendar (ID serial PRIMARY KEY, Day INTEGER);')

    db.query('DROP TABLE IF EXISTS lead_time_history;')
    db.query('CREATE TABLE lead_time_history (ID serial PRIMARY KEY, Day INTEGER, Supplier_Id CHARACTER VARYING (255), Lead_time INTEGER);')

    db.commit()