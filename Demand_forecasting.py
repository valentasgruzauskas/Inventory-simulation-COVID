import fbprophet
import lightgbm as lgb
from scipy.stats import variation
import numpy as np
from scipy.stats import variation 
import pandas as pd
import datetime

from Supplier_selection import generate_data_for_schedule
from Support_functions import get_lead_time, calculate_max_lead_time

def make_orders(self, day, db, short_route_cost, long_route_cost, Inventory_strategy, Optimization_goal):
    
    if Inventory_strategy == "Nayve estimation":
        naive_estimation(db, day)

    if Inventory_strategy == "Nayve estimation and machine learning for buffe size":
        naive_ML_estimation(db, day)

    if Inventory_strategy == "DDMRP":
        DDMRP(db, day, beta = 1.02, gamma = 1.15)

    generate_data_for_schedule(day, db, short_route_cost, long_route_cost, Optimization_goal = Optimization_goal)

    self.order_to_make = day + calculate_max_lead_time(db)

#Demand estimation just by using Prophet
def naive_estimation(db, day):
    
    df_retailer_inventory = db.table_to_df('Retail_inventory')

    #Loop trough all retailers
    for selected_retailer in df_retailer_inventory['agent_id'].unique():

        #select current demand history and create prediction dataframe for the maximum amount of lead-time
        col_names = ['day', 'order_size']

        df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]
        df_active = df_active[col_names]
        current_max_day = df_active['day'].max()
        max_estimated_day = calculate_max_lead_time(db)
        max_estimated_day = max_estimated_day + current_max_day
        known_days = list(df_active['day'].values)
        unknown_days = list(range(current_max_day, max_estimated_day))
        unknown_days = [x+1 for x in unknown_days]
        all_days = known_days + unknown_days
        df_active = df_active.append(df_active.iloc[[-1]*len(unknown_days)])
        df_active['day'] = all_days
        df_active.loc[df_active['day'].isin(unknown_days) == True, 'order_size'] = np.NaN
        all_dates = pd.date_range('2021-01-01', '2031-01-01').tolist()
        all_dates = all_dates[:len(df_active['day'])]
        df_active['day'] = all_dates

        predict_df = df_active[df_active['order_size'].isnull()]
        fit_df = df_active[df_active['order_size'].isnull() == False]
        if len(fit_df) == 1:
            fit_df = fit_df.append([fit_df]*1,ignore_index=True)
            fit_df.loc[1,'day'] = fit_df.loc[1,'day'] + datetime.timedelta(days=1)
            predict_df['day'] = predict_df['day'] + datetime.timedelta(days=1)

        fit_df.columns = ["ds", "y"]
        predict_df.columns = ["ds", "y"]

        #Estiamte demand
        gm_prophet = fbprophet.Prophet(interval_width = 0.95, changepoint_prior_scale = 0.15)
        gm_prophet = gm_prophet.fit(fit_df)
        prediction = gm_prophet.predict(predict_df)
        prediction = prediction[['ds','yhat']]
        prediction['ds'] = unknown_days
        prediction['ds'] = prediction['ds'].astype(int)
        prediction['yhat'] = prediction['yhat'].astype(int)

        #Create full list of values for database
        prediction['agent_id'] = selected_retailer
        prediction['order_size'] = 0
        prediction['stock_level'] = 0
        prediction.columns = ['day', 'estimated_order', 'agent_id', 'order_size', 'stock_level']
        prediction['estimated_order'] = prediction['estimated_order'].apply(lambda x : x if x > 0 else 0)

        #Update demand estimation results to database
        for _, raw in prediction.iterrows():
            db.insert_to_table('INSERT INTO retail_inventory (agent_id, day, stock_level, order_size, estimated_order, backlog, units_sold, storage_costs, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (raw['agent_id'], raw['day'], 0, 0, 0, 0, 0, 0, 0))
            db.update_table_row('UPDATE Retail_inventory SET stock_level = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['stock_level'], raw['agent_id'], raw['day']))
            db.update_table_row('UPDATE Retail_inventory SET order_size = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['order_size'], raw['agent_id'], raw['day']))
            db.update_table_row('UPDATE Retail_inventory SET estimated_order = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['estimated_order'], raw['agent_id'], raw['day']))


#Estimate demand based on Prophet, and residual with LightGBM. The estimated demand + buffer size (residual estiamtion) is set as the order amount
def naive_ML_estimation(db, day):

    df_retailer_inventory = db.table_to_df('Retail_inventory')

    #loop trough all suppliers
    for selected_retailer in df_retailer_inventory['agent_id'].unique():

        #Predict machine learning
        col_names = ['day', 'order_size', 'estimated_order']

        df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]
        df_active = df_active[col_names]

        df_active['residual'] = df_active['order_size'] - df_active['estimated_order']

        col_names = ['day', 'residual']
        df_active = df_active[col_names]

        current_max_day = df_active['day'].max()
        max_estimated_day = calculate_max_lead_time(db)
        max_estimated_day = max_estimated_day + current_max_day
        known_days = list(df_active['day'].values)
        unknown_days = list(range(current_max_day, max_estimated_day))
        unknown_days = [x+1 for x in unknown_days]
        all_days = known_days + unknown_days
        df_active = df_active.append(df_active.iloc[[-1]*len(unknown_days)])
        df_active['day'] = all_days
        df_active.loc[df_active['day'].isin(unknown_days) == True, 'residual'] = np.NaN
       
        predict_df = df_active[df_active['residual'].isnull()]
        fit_df = df_active[df_active['residual'].isnull() == False]
        if len(fit_df) == 1:
            fit_df = fit_df.append([fit_df]*1,ignore_index=True)
            fit_df.loc[1,'day'] = fit_df.loc[1,'day'] + 1
            predict_df['day'] = predict_df['day'] + 1

        LGBMR = lgb.LGBMRegressor()
        LGBMR = LGBMR.fit(np.array(fit_df['day'].values).reshape(-1, 1), np.array(fit_df['residual'].values))
        predict_df['residual'] = LGBMR.predict(np.array(predict_df['day']).reshape(-1, 1))
        predict_df['day'] = unknown_days

        df_ML_prediction = predict_df

        #Predict prohpet
        col_names = ['day', 'order_size']

        df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]
        df_active = df_active[col_names]
        current_max_day = df_active['day'].max()
        max_estimated_day = calculate_max_lead_time(db)
        max_estimated_day = max_estimated_day + current_max_day
        known_days = list(df_active['day'].values)
        unknown_days = list(range(current_max_day, max_estimated_day))
        unknown_days = [x+1 for x in unknown_days]
        all_days = known_days + unknown_days
        df_active = df_active.append(df_active.iloc[[-1]*len(unknown_days)])
        df_active['day'] = all_days
        df_active.loc[df_active['day'].isin(unknown_days) == True, 'order_size'] = np.NaN
        all_dates = pd.date_range('2021-01-01', '2031-01-01').tolist()
        all_dates = all_dates[:len(df_active['day'])]
        df_active['day'] = all_dates

        predict_df = df_active[df_active['order_size'].isnull()]
        fit_df = df_active[df_active['order_size'].isnull() == False]
        if len(fit_df) == 1:
            fit_df = fit_df.append([fit_df]*1,ignore_index=True)
            fit_df.loc[1,'day'] = fit_df.loc[1,'day'] + datetime.timedelta(days=1)
            predict_df['day'] = predict_df['day'] + datetime.timedelta(days=1)

        fit_df.columns = ["ds", "y"]
        predict_df.columns = ["ds", "y"]

        #Estiamte demand
        gm_prophet = fbprophet.Prophet(interval_width = 0.95, changepoint_prior_scale = 0.15)
        gm_prophet = gm_prophet.fit(fit_df)
        prediction = gm_prophet.predict(predict_df)
        prediction = prediction[['ds','yhat']]
        prediction['ds'] = unknown_days
        prediction['ds'] = prediction['ds'].astype(int)
        prediction['yhat'] = prediction['yhat'].astype(int)
        prediction.columns = ['day', 'estimated_order']

        #Prophet + ML
        prediction = prediction.merge(df_ML_prediction, on='day')
        prediction['estimated_order'] = prediction['estimated_order'] + prediction['residual']

        #Create full list of values for database
        prediction['agent_id'] = selected_retailer
        prediction['order_size'] = 0
        prediction['stock_level'] = 0
        prediction = prediction.drop(columns=['residual'])
        prediction.columns = ['day', 'estimated_order', 'agent_id', 'order_size', 'stock_level']
        prediction['estimated_order'] = prediction['estimated_order'].astype(int)
        prediction['estimated_order'] = prediction['estimated_order'].apply(lambda x : x if x > 0 else 0)

        #Update demand estimation results to database
        for _, raw in prediction.iterrows():
            db.insert_to_table('INSERT INTO retail_inventory (agent_id, day, stock_level, order_size, estimated_order, backlog, units_sold, storage_costs, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (raw['agent_id'], raw['day'], 0, 0, 0, 0, 0, 0, 0))
            db.update_table_row('UPDATE Retail_inventory SET stock_level = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['stock_level'], raw['agent_id'], raw['day']))
            db.update_table_row('UPDATE Retail_inventory SET order_size = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['order_size'], raw['agent_id'], raw['day']))
            db.update_table_row('UPDATE Retail_inventory SET estimated_order = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['estimated_order'], raw['agent_id'], raw['day']))

#Estimate demand with Prophet and set buffer size based on DDMRP
def DDMRP(db, day, beta, gamma):

    df_retailer_inventory = db.table_to_df('Retail_inventory')

    #loop trough all retailers
    for selected_retailer in df_retailer_inventory['agent_id'].unique():

        #Safety stock level from DDMRP
        col_names = ['day', 'order_size']

        df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]
        df_active = df_active[col_names]
        current_max_day = df_active['day'].max()
        max_estimated_day = calculate_max_lead_time(db)
        max_estimated_day = max_estimated_day + current_max_day
        known_days = list(df_active['day'].values)
        unknown_days = list(range(current_max_day, max_estimated_day))
        unknown_days = [x+1 for x in unknown_days]
        all_days = known_days + unknown_days
        df_active = df_active.append(df_active.iloc[[-1]*len(unknown_days)])
        df_active['day'] = all_days
        df_active.loc[df_active['day'].isin(unknown_days) == True, 'order_size'] = np.NaN
        all_dates = pd.date_range('2021-01-01', '2031-01-01').tolist()
        all_dates = all_dates[:len(df_active['day'])]
        df_active['day'] = all_dates

        predict_df = df_active[df_active['order_size'].isnull()]
        fit_df = df_active[df_active['order_size'].isnull() == False]
        if len(fit_df) == 1:
            fit_df = fit_df.append([fit_df]*1,ignore_index=True)
            fit_df.loc[1,'day'] = fit_df.loc[1,'day'] + datetime.timedelta(days=1)
            predict_df['day'] = predict_df['day'] + datetime.timedelta(days=1)

        CV_D = variation(df_active['order_size'].dropna())
        CV_L = variation(get_lead_time(db))
        d = df_active['order_size'].mean()
        L = np.mean(get_lead_time(db))

        RB = d * (beta * np.sqrt(L) + gamma)
        RS = RB * np.sqrt(CV_D ** 2 + CV_L ** 2 * L)

        SS = RB + RS
        SS = int(SS / max_estimated_day)

        #Predict prohpet
        col_names = ['day', 'order_size']

        df_active = df_retailer_inventory[df_retailer_inventory['agent_id'] == selected_retailer]
        df_active = df_active[col_names]
        current_max_day = df_active['day'].max()
        max_estimated_day = calculate_max_lead_time(db)
        max_estimated_day = max_estimated_day + current_max_day
        known_days = list(df_active['day'].values)
        unknown_days = list(range(current_max_day, max_estimated_day))
        unknown_days = [x+1 for x in unknown_days]
        all_days = known_days + unknown_days
        df_active = df_active.append(df_active.iloc[[-1]*len(unknown_days)])
        df_active['day'] = all_days
        df_active.loc[df_active['day'].isin(unknown_days) == True, 'order_size'] = np.NaN
        all_dates = pd.date_range('2021-01-01', '2031-01-01').tolist()
        all_dates = all_dates[:len(df_active['day'])]
        df_active['day'] = all_dates

        predict_df = df_active[df_active['order_size'].isnull()]
        fit_df = df_active[df_active['order_size'].isnull() == False]
        if len(fit_df) == 1:
            fit_df = fit_df.append([fit_df]*1,ignore_index=True)
            fit_df.loc[1,'day'] = fit_df.loc[1,'day'] + datetime.timedelta(days=1)
            predict_df['day'] = predict_df['day'] + datetime.timedelta(days=1)

        fit_df.columns = ["ds", "y"]
        predict_df.columns = ["ds", "y"]

        #Estiamte demand
        gm_prophet = fbprophet.Prophet(interval_width = 0.95, changepoint_prior_scale = 0.15)
        gm_prophet = gm_prophet.fit(fit_df)
        prediction = gm_prophet.predict(predict_df)
        prediction = prediction[['ds','yhat']]
        prediction['ds'] = unknown_days
        prediction['ds'] = prediction['ds'].astype(int)
        prediction['yhat'] = prediction['yhat'].astype(int)
        prediction.columns = ['day', 'estimated_order']

        #Prophet + DDMRP
        prediction['estimated_order'] = prediction['estimated_order'] + SS

        #Create full list of values for database
        prediction['agent_id'] = selected_retailer
        prediction['order_size'] = 0
        prediction['stock_level'] = 0
        prediction.columns = ['day', 'estimated_order', 'agent_id', 'order_size', 'stock_level']
        prediction['estimated_order'] = prediction['estimated_order'].apply(lambda x : x if x > 0 else 0)

        #Update demand estimation results to database
        for _, raw in prediction.iterrows():
            db.insert_to_table('INSERT INTO retail_inventory (agent_id, day, stock_level, order_size, estimated_order, backlog, units_sold, storage_costs, revenue) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', (raw['agent_id'], raw['day'], 0, 0, 0, 0, 0, 0, 0))
            db.update_table_row('UPDATE Retail_inventory SET stock_level = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['stock_level'], raw['agent_id'], raw['day']))
            db.update_table_row('UPDATE Retail_inventory SET order_size = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['order_size'], raw['agent_id'], raw['day']))
            db.update_table_row('UPDATE Retail_inventory SET estimated_order = (%s) WHERE agent_id = (%s) AND day = (%s)', (raw['estimated_order'], raw['agent_id'], raw['day']))

   
