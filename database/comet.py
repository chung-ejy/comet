from database.adatabase import ADatabase
import pandas as pd
class Comet(ADatabase):
    
    def __init__(self):
        super().__init__("comet")
    

    def retrieve_fills(self):
        try:
            db = self.client[self.name]
            table = db["cloud_test_fills"]
            data = table.find({},{"order_id":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))
    
    def retrieve_incomplete_trade(self,order_id):
        try:
            db = self.client[self.name]
            table = db["cloud_test_incomplete_trades"]
            data = table.find({"sell_id":order_id},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"incomplete_trades",str(e))