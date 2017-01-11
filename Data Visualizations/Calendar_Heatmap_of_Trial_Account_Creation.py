df = datasets["Calendar"]
df["week"] = pd.to_datetime(df["week"]).astype(str)
df = df[["week", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]]
df.columns = ["Week", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

bg = df.copy()
bg["Sunday"] = (df["Sunday"] - df["Sunday"].mean())/df["Sunday"].std()
bg["Monday"] = (df["Monday"] - df["Monday"].mean())/df["Monday"].std()
bg["Tuesday"] = (df["Tuesday"] - df["Tuesday"].mean())/df["Tuesday"].std()
bg["Wednesday"] = (df["Wednesday"] - df["Wednesday"].mean())/df["Wednesday"].std()
bg["Thursday"] = (df["Thursday"] - df["Thursday"].mean())/df["Thursday"].std()
bg["Friday"] = (df["Friday"] - df["Friday"].mean())/df["Friday"].std()
bg["Saturday"] = (df["Saturday"] - df["Saturday"].mean())/df["Saturday"].std()

# df = df.replace(np.nan,' ', regex=True)
df = df.set_index('Week')
bg = bg.set_index('Week')

plt.figure(figsize=(18, 4))                           # specify width, height

sns.set(font_scale=2)                                 # re-size the text used in the heat map
yticks = bg.index
xticks = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

colors = sns.light_palette("#2ecc71", as_cmap=True)

ax_2 = sns.heatmap(data = bg, 
            annot = False,                              # turn off the annotations for the standard deviation dataset
            linewidths = .5, 
            #cmap = colors,
            cmap = "RdYlGn",                          # specify the green -> red color scheme
            xticklabels = xticks,
            yticklabels = yticks,
            cbar_kws={"orientation": "vertical"}      # re-locate the legend to the bottom of the plot
            )
ax_1 = sns.heatmap(data=df, 
            annot = True,                               # turn on the annotations for the standard deviation dataset
            alpha = 0.0, 
            # fmt = "d",                                  # format the value inputs to avoid exponential representation
            linewidths = .5,
            cbar = False,
            annot_kws = {"size": 25, "color":'black'},
            xticklabels = xticks,
            yticklabels = yticks,
            cbar_kws={"orientation": "vertical"}      # re-locate the legend to the bottom of the plot
            )

ax_1.set_ylabel('')                                     # Remove the y-axis title
ax_1.set_xlabel('')                                     # Remove the x-axis title

for item in ax_1.get_xticklabels():
    item.set_rotation(45)