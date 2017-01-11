library(shiny)
library(rsconnect)
rsconnect::setAccountInfo(name='stitchlabs',
                          token='423F4304FC2B7DA34E2CBDA6034405EE',
                          secret='lDUCzYDrm4TydiBieUoY1/r9U8fofJNFiPZ5ULSl')
# Specify the local directory
# setwd("/Users/benjaminknight/Documents/Stitch/Hax\ Day\ 2016/Hax-Day-2016-master")
# Read in the data
df <- read.csv("combined_df.csv")
df$X <- NULL
# Remove outliers
df <- df[ which(df$order < 3000), ]
df <- df[ which(df$minutes_in_app < 1200), ]
# Load the necessary packages
library(shiny)
library(dplyr)
library(car)
# Sample the data as necessary for performance reasons
df <- sample_n(df, 20000)
# Only include onboarded accounts
df <- df[ which(df$active_and_onboarded_on_15JUL16 == 1), ]

###################################################################
# UI Function                                                     #
###################################################################
# The dom created by the rshiny headerpanel function will not
# allow full utilization of the company css as stipulated in the
# style guide. The HTML for the header panel is explicitly laid
# out below. 

ui <- shinyUI(
        fluidPage(theme = "timeinapp_styles.css",
          tags$div(class="row",
            tags$div(class="col-sm-12",
              tags$h1("Typical Time In-App by Stitch Customers Vs. Scale of Operations")
            )
          ),  
        # allows input of # of SKUs
        tags$div(class="row",
          tags$div(class="col-sm-12",
            tags$form(class="well",
                   # R is not processing the "for" argument and is generating an error
                   #   <div class="form-group shiny-input-container">
                   #     <label class="control-label" for="sku_class">Number of SKUs:</label>
                   #       <div>
                   # tags$div(class="form-group shiny-input-container",
                   #(tags$label(class="control-label" for="sku_class"), Number of SKUs)
                   #      ),
               tags$div(class="selectize-control single",
         selectInput("sku_class", "Number of Active SKUs:", 
         c("All", "0-999 SKUS", "1,000-1,999 SKUS", "2,000-2,999 SKUS", 
           "3,000-3,999 SKUS","+4,000 SKUS")),
                     numericInput("orders_num", "Number of Orders:", NA),
                     numericInput("time", "Hours Expended:", NA),
                     width = 3 #, # width of the side panel
         
         # adding the legend to the sidebar            
       #  tags$div(
       #    "Some text followed by a break", 
      #     tags$br(),
        #   "Some text following a break"
          #    )
            )
          )
        )
        ),
        # The main plot 
        tags$div(class="col-sm-8",
                tags$div(id="plot", 
                         class="shiny-plot-output shiny-bound-output",
                         style="width: 100% ; height: 400px",
                plotOutput('plot'),
                tags$span(class="help-block",
                helpText("Data is a random sample of 20,000 customer/weeks",
                         "taken from active Stitch customers",
                         "(as of 7/15/16) during weeks of operation",
                         "January 2013 - June 2016.",
                         "Outliers are omitted where order volume is",
                         "greater than 3,000 or where",
                         "time in-app is greater than 20 hours per week.",
                         "Only includes onboarded accounts where a minimum",
                         "of 90 days has elapsed since a customer's billing",
                         "start date. Non-parametric smoothing is by",
                         "loess (non-parametric local regression).")
                )
                )
        )
))

###################################################################
# Server Function                                                 #
###################################################################
server <- shinyServer(function(input, output) {
        Data_Select <- reactive({
                if (input$sku_class != "All") 
                {df <- df %>% filter(sku_group == input$sku_class)}
                df
                })
        output$plot <- renderPlot({
        library(ggplot2)
        library(extrafont)
        p <- ggplot(Data_Select(),                                # specify the dataset
                    aes(orders, round(minutes_in_app/60, 2)),     # specify the variables
                    show.legend=TRUE)     
        p <- p + geom_point(colour = "#ff653a", alpha = 0.5)      # add points
        p <- p + labs(title= "Weeks of Operations by Stitch Customers",
                      x="\nNumber of Orders in a Given Week",   
                      y="Number of Active SKUs in a Given Week\n")  # add axis labels
                # we can add an \n in front of the x-axis label
                # and a \n after the y-axis label to provide additional padding
        p <- p + xlim(c(0, 3000))                          # delimit x-axis
        p <- p + ylim(c(0, 20))                            # delimit y-axis
        p <- p + scale_x_continuous(
                    breaks=c(-250,0,250,500,750,
                             1000,1250,1500,1750,
                             2000,2250,2500,2750,3000))    # specify x ticks
        p <- p + scale_y_continuous(
                    breaks=c(0,2.5,5,7.5,10,
                             12.5,15,17.5,20))             # specify y ticks
        p <- p + theme(title =                             # specify title style
                    element_text(family = "Helvetica",     # title font
                    color="#000000",                       # title color
                    face="bold",                           # title style
                    size=13))                              # title size
        p <- p + theme(axis.title.y =                      # specify y axis style  
                    element_text(family = "Lucida Grande", # y-axis font
                    hjust=0.5,                             # horizontal justification
                    vjust=0.5,                             # vertical justification
                    color="#000000",                       # y-axis label color
                    face="bold",                           # y-axis label style
                    size=13))                              # y-axis label size
        p <- p + theme(axis.title.x =                      # specify x axis style
                    element_text(family = "Lucida Grande", # x-axis font
                    hjust=0.5,                             # horizontal justification
                    vjust=0.5,                             # vertical justification
                    color="#000000", 
                    face="bold", 
                    size=13)) 
        p <- p + scale_shape_identity()                    # allow shape specification
        p <- p + geom_point(data=
                    Data_Select(),                         # specify the dataset
                    aes(x = as.numeric(input$orders_num),  # dot x coordinate
                        y = as.numeric(input$time),        # dot y coordinate
                        shape = 21),                       # specify point shape
                    fill="#4990e1",                        # dot color
                    colour="#4990e1",                      # dot outline
                    alpha = 0.2,
                    size=5)                                # specify color/size
        p <- p + stat_smooth(
                    fill = "#4990e1",                      # specify color
                    alpha = 0.2,                           # specify transparency
                    show.legend = TRUE) 
# Specify plot background (within plot + padding)
        p <- p + theme_bw()  
    #   p <- p + theme(plot.background = 
    #               element_rect(fill = "#f5f5f5"))        # specify background color
        print(p)                                           # print
        })
})

###################################################################
# Combine the server and UI functions                             #
###################################################################
shinyApp(ui=ui,server=server)



