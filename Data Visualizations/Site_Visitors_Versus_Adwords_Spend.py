import matplotlib.pyplot as plt
import matplotlib.dates as dates

df1 = datasets["Visitors"]
x1 = df1["date"]

x1 = x1.sort_values(axis=0, ascending=True)
y1 = df1["marketing site visitors"]
df2 = datasets["Adwords Spend"]
x2 = df2["date"]
y2 = df2["adwords spend"]

# Create the Figure
fig = plt.figure()                                 # create figure
fig.set_size_inches(15.0, 4.5)                     # specify figure width and height

# Specify the tick label sizes
label_size = 13
plt.rcParams['xtick.labelsize'] = label_size 

# Add the line graphs
line1 = plt.plot(x1, y1, label = "Unique Visitors to Marketing Site (Non-Blog)") # overlay one line graph 
line2 = plt.plot(x1, y2, label = "Adwords Spend in Thousands USD")               # overlay the adwords line graph

# Add labels
plt.xlabel("Date", fontsize=18)
plt.ylabel("Visitors / Adwords Spend in USD", fontsize=15)
# plt.title("Marketing Site Visitors (Non-Blog) Vs. Google Adwords Spend in the Past 90 Days", fontsize=22)
plt.legend()

# annotate the figure
ax = fig.add_subplot(111)
ax.annotate('New Website Launch', (dates.datestr2num('2016-10-05'), 2176), xytext=(-68, -80), size=13, \
            textcoords='offset points', arrowprops=dict(arrowstyle='-|>'))
ax.annotate('Thanksgiving', (dates.datestr2num('2016-11-24'), 1787), xytext=(-43, -70), size=13, \
            textcoords='offset points', arrowprops=dict(arrowstyle='-|>'))
ax.annotate('Christmas', (dates.datestr2num('2016-12-25'), 	1510), xytext=(-33, -60), size=13, \
            textcoords='offset points', arrowprops=dict(arrowstyle='-|>'))



# # create a grid
plt.grid(True)

# # show results 
fig.autofmt_xdate()
plt.show()