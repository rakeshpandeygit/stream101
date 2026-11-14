import json
from tabulate import tabulate

with open("data/order.json") as f:
    data = json.load(f)

lst = []
for i in data['items']:
    row = {
        "order_id": data["order_id"],
        "customer_id": data["customer"]["customer_id"],
        "name": data["customer"]["name"],
        "product_id":i["product_id"],
        "product_name":i["product_name"],
        "price":i["price"],
        "city":data["address"]["city"],
        "state":data["address"]["state"]

    }

    lst.append(row)


print(tabulate(lst, headers="keys", tablefmt="grid"))