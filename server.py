from mesa.visualization.ModularVisualization import ModularServer
from mesa.visualization.UserParam import UserSettableParameter
from mesa.visualization.modules import ChartModule
from mesa.visualization.modules import NetworkModule
from model import InventorySimulation

def supply_network(G):

    #define the data for vizualization
    portrayal = dict()
    portrayal["nodes"] = [
        {
            "id": node_id,
            "size": 3,
            "color": "#CC0000" if node_id[0] == 'S' else '#ff5100' if node_id[0] == 'W' else '#616BF9' if node_id[0] == 'A' else '#F9F142',
            "label": None
        }
        for (node_id, agents) in G.nodes.data("agent")
    ]

    portrayal["edges"] = [
        {"id": edge_id,
        "source": edge[0],
        "target": edge[1],
        'distance': edge[2]['distance'],
        "color": "#000000"}
        for edge_id, edge in enumerate(G.edges(data=True))
    ]

    return portrayal

#Create network
grid = NetworkModule(supply_network, 750, 750, library="sigma")

#Define graphs
inventory_chart = ChartModule(
    [
        {"Label": "Total orders", "Color": "#FF0000"},
        {"Label": "Total inventory", "Color": "#008000"}
    ]
)

order_fulfilment_chart = ChartModule(
    [
        {"Label": "Order fulfilment", "Color": "#FF0000"}
    ]
)

lead_time_chart = ChartModule(
    [
        {"Label": "Lead time short route", "Color": "#FF0000"},
        {"Label": "Lead time long route", "Color": "#008000"}
    ]
)

financial_chart = ChartModule(
    [
        {"Label": "Revenue", "Color": "#FF0000"},
        {"Label": "Cost of production", "Color": "#ffe100"},
        {"Label": "Costs of raw material", "Color": "#11eb09"},
        {"Label": "Cost of delivery", "Color": "#88138a"},
        {"Label": "Profit", "Color": "#30eeff"}
    ]
)

#define input parameters for scenario selection
model_params = {
    "Inventory_strategy": UserSettableParameter(
        'choice',
        'Inventory strategy',
        value='Nayve estimation',
        choices = ["Nayve estimation", "Nayve estimation and machine learning for buffe size", "DDMRP"]
    ),
    
    "Optimization_goal":  UserSettableParameter(
        'choice',
        'Optimization goal',
        value='Profit',
        choices = ["Profit", "Lead-time", "On time delivery"]
    ),
        "Disruption_intensity": UserSettableParameter(
            'slider',
            'Disruption propability',
            value = 0.15,
            min_value = 0,
            max_value = 1,
            step = 0.01
    ), 
}

#Run the simulation using Mesa
server = ModularServer(
    InventorySimulation, [grid, inventory_chart, lead_time_chart, order_fulfilment_chart, financial_chart], "Inventory simulation", model_params
)
server.port = 8500