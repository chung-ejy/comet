
class Entry(object):
    @classmethod
    def standard(self,final,date,signal,value,conservative):
        if value:
            offerings = final[(final["date"]==date) 
                                & (final["signal"] < -signal)
                                ].sort_values("signal",ascending=conservative)
        else:
            sorting = not conservative
            offerings = final[(final["date"]==date) 
                                & (final["signal"] > signal)
                                ].sort_values("signal",ascending=sorting)
        return offerings
    
    @classmethod
    def research_parameter_defined(self,final,date,signal,value,conservative):
        if value:
            offerings = final[(final["date"]==date) 
                                & (final["signal"] < -signal)
                                & (final["velocity"] >= -3)
                                & (final["velocity"] < 0)
                                & (final["inflection"] >= -1)
                                & (final["inflection"] <= 1)
                                ].sort_values("signal",ascending=conservative)
        else:
            sorting = not conservative
            offerings = final[(final["date"]==date) 
                                & (final["signal"] > signal)
                                & (final["velocity"] > 0)
                                & ((final["inflection"] <= 1)
                                | (final["inflection"] >= -1))
                                ].sort_values("signal",ascending=sorting)
        return offerings
    
    @classmethod
    def parameter_defined(self,final,date,signal,value,conservative):
        if value:
            offerings = final[(final["date"]==date) 
                                & (final["signal"] < -signal)
                                & (final["velocity"] >= -3)
                                & (final["velocity"] < 0)
                                & (final["inflection"] >= -1)
                                & (final["inflection"] <= 1)
                                ].sort_values("signal",ascending=conservative)
        else:
            sorting = not conservative
            offerings = final[(final["date"]==date) 
                                & (final["signal"] > signal)
                                & (final["velocity"] > 0)
                                & ((final["inflection"] >= 1)
                                & (final["inflection"] <= -1))
                                ].sort_values("signal",ascending=sorting)
        return offerings

    @classmethod
    def signal_based(self,final,date,signal,value,conservative):
        if value:
            offerings = final[(final["date"]==date) 
                                & (final["signal"] < -signal)
                                & (final["p_sign_change"]==True)
                                ].sort_values("signal",ascending=conservative)
        else:
            sorting = not conservative
            offerings = final[(final["date"]==date) 
                                & (final["signal"] > signal)
                                & (final["p_sign_change"]==True)
                                ].sort_values("signal",ascending=sorting)
        return offerings
    
    @classmethod
    def all(self,final,date,signal,value,conservative):
        if value:
            offerings = final[(final["date"]==date) 
                                & (final["signal"] < -signal)
                                & (final["p_sign_change"]==True)
                                & (final["velocity"] >= -3)
                                & (final["velocity"] < 0)
                                & (final["inflection"] >= 0)
                                & (final["inflection"] <= 1)
                                ].sort_values("signal",ascending=conservative)
        else:
            sorting = not conservative
            offerings = final[(final["date"]==date) 
                                & (final["signal"] > signal)
                                & (final["p_sign_change"]==True)
                                 & (final["velocity"] > 0)
                                & ((final["inflection"] <= 0)
                                | (final["inflection"] >= -1))
                                ].sort_values("signal",ascending=sorting)
        return offerings