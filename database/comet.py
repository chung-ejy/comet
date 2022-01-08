from database.adatabase import ADatabase
import pandas as pd
class Comet(ADatabase):
    
    def __init__(self):
        super().__init__("comet")
