import pandas as pd
import matplotlib.pyplot as plt

class StatChecker():
    """
    Python API to queury from AQS database.
    """
    # hey 

    def __init__(self, data):
        """
        initializes the object with the dataframe you want checked
        """
        # params for checking for Nans if needed
        # self.params = {
        #     'email': "orussell@g.hmc.edu",
        #     'key': "silverwren87"}

        # dataframe WITH QUALIFIERS
        self.df = pd.DataFrame(data=data)

    def graphNaNTypes(self, dataLabel, timeFrame):
        """
        @param df - the dataframe 
        @param dataLabel - the name of the column of data to analyze (eg "Ozone")
        @param timeFrame - the timeframe to graph by (eg "month")
        @return (graph, nanDictList, numNaNsTotal)
            graph - the graph generated
            nanDictList - the list of dictionaries containing the type of NaN as a key and number of NaNs of that type as the value for each timeframe
            numNaNsTotal - a list of the total number of NaNs in each timeframe
        """
        
        if timeFrame == "year":
            sYear = self.df.iloc[0].name.year
            eYear = self.df.iloc[-1].name.year
            xAxis = list(range(sYear, eYear+1))
        elif timeFrame == "season":
            xAxis = ["Jan-Feb-Mar", "Apr-May-Jun", "Jul-Aug-Sep", "Oct-Nov-Dec"]
        elif timeFrame == "month":
            xAxis = ["Jan", "Feb", "March", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        elif timeFrame == "weekday":
            xAxis = ["Mon", "Tues", "Wed", "Thurs", "Fri", "Sat", "Sun"]
        elif timeFrame == "day":
            xAxis = list(range(1, 32))
        elif timeFrame == "hour":
            xAxis = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', 
                '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00',
                '20:00', '21:00', '22:00', '23:00']
        else:
            pass # throw an error maybe?

        xAxisLen = len(xAxis)
        
        # edited df to contain only NaN values
        nanDF = self.df[self.df[dataLabel].isna()]

        # list of dictionaries to easily store data and search by message
        nanDictList = []

        # also keeping parallel arrays so can create histogram
        # parallel arrays len(messageList) == len(valueList)
        # with each message corrosponding to the spot in valueList that holds the list of number of that message for each hour
        messageList = []
        valueList = []
        numNaNsTotal = [0]*xAxisLen

        for xIndex in range(xAxisLen):
            # dataframe for specific hour/day/month/etc
            # currentDF = nanDF.loc[lambda row: row['time_local'] == hours[hourIndex]] # TODO: fix this

            if timeFrame == "year":
                currentDF = nanDF.loc[nanDF.index.year == xIndex + sYear]
            elif timeFrame == "season":
                currentDF = nanDF.loc[(nanDF.index.month >= xIndex*3 + 1) & (nanDF.index.month <= xIndex*3 + 3)]
            elif timeFrame == "month":
                currentDF = nanDF.loc[nanDF.index.month == xIndex + 1]
            elif timeFrame == "weekday":
                currentDF = nanDF.loc[nanDF.index.weekday == xIndex]
            elif timeFrame == "day":
                currentDF = nanDF.loc[nanDF.index.day == xIndex + 1]
            elif timeFrame == "hour":
                currentDF = nanDF.loc[nanDF.index.hour == xIndex]
            else:
                pass # throw an error maybe?


            nanDictList.append({})
            for message in currentDF[dataLabel+' - qualifier']:
                numNaNsTotal[xIndex] += 1

            if message in nanDictList[-1]:
                nanDictList[-1][message] += 1
            else:
                nanDictList[-1][message] = 1

            if message not in messageList:
                valueList.append([0]*xAxisLen)
                messageList.append(message)
        
            valueList[messageList.index(message)][xIndex] += 1

        graph = plt.figure(figsize = [15, 6])

        plt.bar(xAxis, valueList[0])
        bottoms = valueList[0]
        for i in range(1, len(valueList)):
            plt.bar(xAxis, valueList[i], bottom=bottoms)
            bottoms = list(map(lambda x, y: x+y, bottoms, valueList[i]))

        plt.legend(messageList)
    
        return (graph, nanDictList, numNaNsTotal)


    def extreme_yearly(self, measurement, units, threshold=80) :
        yrStart = self.df.iloc[0].name.year
        yrEnd = self.df.iloc[-1].name.year

        extreme_df = self[(df[measurement] > 0)][measurement]
        fig, ax = plt.subplots(figsize=(20,12))
        columns = []
        for yr in range(yrStart, yrEnd) :
            year_df = extreme_df[extreme_df.index.year == yr]
            columns.append(year_df)

        ax.boxplot(columns)
        plt.xticks(list(range(1,yrEnd-yrStart+1)), list(range(yrStart, yrEnd)), rotation=30)
        ax.set_title('Extreme values for ' + measurement, fontsize=16, weight='bold')
        ax.set_xlabel('Year', fontsize=14)
        ax.set_ylabel(measurement + ' ' + units, fontsize=14)
        plt.axhline(threshold, color='r', linestyle='--')

        return plt


    def yearly_avg(self, measurement) :
        yrStart = self.df.iloc[0].name.year
        yrEnd = self.df.iloc[-1].name.year

        avg_df = pd.DataFrame(columns=[measurement + "_avg"], index=range(yrStart,yrEnd))
        avg_df.index.name = 'Year'
        for yr in range(yrStart, yrEnd) :
            avg_df[measurement + "_avg"][yr] = df[df.index.year == yr][measurement].mean()
        avg_df.plot(linestyle='-', marker='o', figsize=(12,6))
        
        return plt

    def getMonths(input, m1, m2, m3) :
        return input.loc[(input.index.month==m1) | (input.index.month==m2) | (input.index.month==m3)]


    def seasonal_avg(self, yrStart, yrEnd, measurement, ylim) :
        spring_df = getMonths(self, 3,4,5)[measurement]
        spring_df = spring_df.groupby(spring_df.index.year).describe()
        spring_df['mean_minstd'] = spring_df['mean'] - spring_df['std']
        spring_df['mean_plusstd'] = spring_df['mean'] + spring_df['std']

        summer_df = getMonths(self, 6,7,8)[measurement]
        summer_df = summer_df.groupby(summer_df.index.year).describe()
        summer_df['mean_minstd'] = summer_df['mean'] - summer_df['std']
        summer_df['mean_plusstd'] = summer_df['mean'] + summer_df['std']

        fall_df = getMonths(self, 9,10,11)[measurement]
        fall_df = fall_df.groupby(fall_df.index.year).describe()
        fall_df['mean_minstd'] = fall_df['mean'] - fall_df['std']
        fall_df['mean_plusstd'] = fall_df['mean'] + fall_df['std']

        winter_df = getMonths(self, 12,1,2)[measurement]
        winter_df = winter_df.groupby(winter_df.index.year).describe()
        winter_df['mean_minstd'] = winter_df['mean'] - winter_df['std']
        winter_df['mean_plusstd'] = winter_df['mean'] + winter_df['std']

        seasonal_df = [spring_df, summer_df, fall_df, winter_df]
        seasonal_labels = ['Spring', 'Summer', 'Fall', 'Winter']

        fig, axs = plt.subplots(2, 2, figsize=(20,12))

        for i, ax in enumerate(fig.axes) :

            ax.set_title(seasonal_labels[i] + ' Mean Profile for ' + measurement, fontsize=16, weight='bold')
            ax.plot(seasonal_df[i].index, seasonal_df[i]['mean'], color='g', linewidth=3.0)
            ax.plot(seasonal_df[i].index, seasonal_df[i]['mean_plusstd'], color='g')
            ax.plot(seasonal_df[i].index, seasonal_df[i]['mean_minstd'], color='g')
            ax.fill_between(seasonal_df[i].index, seasonal_df[i]['mean'], seasonal_df[i]['mean_plusstd'], alpha=.5, facecolor='g')
            ax.fill_between(seasonal_df[i].index, seasonal_df[i]['mean'], seasonal_df[i]['mean_minstd'], alpha=.5, facecolor='g')
            ax.set_xlabel('Year', fontsize=14)
            ax.set_ylabel(measurement, fontsize=14)
            ax.set_xlim(yrStart, yrEnd-1)
            ax.set_ylim(0, ylim)

        return plt
        
