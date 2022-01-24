from database.adatabase import ADatabase
import pandas as pd
class Comet(ADatabase):
    
    def __init__(self,version):
        super().__init__("comet")
        self.version = version

    def retrieve_fills(self,user):
        try:
            db = self.client[self.name]
            table = db["cloud_{self.version}_fills"]
            data = table.find({"username":user},{"order_id":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))
    
    def retrieve_completed_buys(self,user):
        try:
            db = self.client[self.name]
            table = db["cloud_{self.version}_completed_buys"]
            data = table.find({"username":user},{"order_id":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))
    
    def retrieve_pending_trades(self,user):
        try:
            db = self.client[self.name]
            table = db["cloud_{self.version}_pending_trades"]
            data = table.find({"username":user},{"order_id":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))

    def retrieve_completed_trades(self,user):
        try:
            db = self.client[self.name]
            table = db["cloud_{self.version}_completed_trades"]
            data = table.find({"username":user},{"order_id":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))
    
    def retrieve_completed_sells(self,user):
        try:
            db = self.client[self.name]
            table = db["cloud_{self.version}_completed_sells"]
            data = table.find({"username":user},{"order_id":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,"fills",str(e))
