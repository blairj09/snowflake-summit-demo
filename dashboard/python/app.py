import chatlas, querychat, snowflake.connector, os
import polars as pl
import plotly.express as px

from querychat.datasource import SQLAlchemySource
from pathlib import Path
from shiny import App, render, ui, reactive, session
from shinywidgets import output_widget, render_widget
from dotenv import load_dotenv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from posit.connect import Client


load_dotenv()

# querychat config ----
with open(Path(__file__).parent / "greeting.md", "r") as f:
    greeting = f.read()
with open(Path(__file__).parent / "data_description.md", "r") as f:
    data_desc = f.read()
with open(Path(__file__).parent / "instructions.md", "r") as f:
    instructions = f.read()

def anthropic_chat(system_prompt: str) -> chatlas.Chat:
    return chatlas.ChatAnthropic(
        model="claude-3-5-sonnet-latest", system_prompt=system_prompt
    )

account = os.getenv("SNOWFLAKE_ACCOUNT")
# Configure OAuth within Posit Connect
if os.getenv("RSTUDIO_PRODUCT") == "CONNECT":
    user_session_token = session.http_conn.headers.get("Posit-Connect-User-Session-Token")
    oauth_token = Client().oauth.get_credentials(user_session_token).get("access_token")

    snowflake_connection = SQLAlchemySource(
        create_engine(URL(
            account = account,
            token = oauth_token,
            authenticator = "oauth",
            database = "DEMOS",
            schema = "PUBLIC",
            warehouse = "DEFAULT_WH",
        )),
    "AIR_QUALITY_FILTERED")
else:
    querychat_config = querychat.init(
        # Snowflake connection
        SQLAlchemySource(
            create_engine(URL(
                account = account,
                # connection_name comes from the config file created automatically by Posit Workbench
                connection_name = "workbench",
                database = "DEMOS",
                schema = "PUBLIC",
                warehouse = "DEFAULT_WH",
                role = "DEVELOPER"
            )),
        "AIR_QUALITY_FILTERED"),
        greeting=greeting,
        data_description=data_desc,
        extra_instructions=instructions,
        create_chat_callback=anthropic_chat
    )

# Suplemental data ----
state_abbreviations = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District Of Columbia": "DC",
}

# Shiny App ----
# Create UI
app_ui = ui.page_sidebar(
    # Create sidebar chat
    querychat.sidebar(
        "chat"
    ),
    # Main panel with data viewer and plots
    ui.layout_columns(
        # Row 1
        ui.card(
            ui.card_header("Current Filter"), 
            ui.output_text("filtered_metric"), 
            fill=True
        ),
        # Row 2
        ui.card(
            ui.card_header("Geographic Distribution"), 
            output_widget("plot_map"), 
            fill=True
        ),
        # Row 3
        ui.card(
            ui.card_header("Temporal Trends"), 
            output_widget("plot_line"), 
            fill=True
        ),
        # Row 4
        ui.card(
            ui.card_header("Distribution"), 
            output_widget("plot_hist"), 
            fill=True
        ),
        ui.card(
            ui.card_header("Percentile Analysis"), 
            output_widget("plot_scatter"), 
            fill=True
        ),
        # Row 5
        ui.card(
            ui.card_header("Raw Data"), 
            ui.output_data_frame("data_table"), 
            fill=True
        ),
        col_widths=[12, 12, 6, 6, 12],
    ),
    title="US Air Quality",
    fillable=True,
)


# Define server logic
def server(input, output, session):
    # Initialize querychat server with the config
    chat = querychat.server("chat", querychat_config)

    # Create reactive value for dataframe
    @reactive.calc
    def df():
        df_pl = pl.from_dataframe(chat["df"]())
        return df_pl

    @reactive.calc
    def filter_name():
        # We choose only the first value, assuming all values are the same.
        # This assumption is accurate except for when the application is first initialized
        return df().select("parametername").row(0)[0]

    @render.text
    def filtered_metric():
        return filter_name()

    @render_widget
    def plot_map():
        data = (
            df()
            .group_by("statename")
            .agg(pl.col("arithmeticmean").mean())
            .with_columns(
                pl.col("statename").replace(state_abbreviations).alias("stateabb")
            )
        )

        map_output = px.choropleth(
            data,
            locations="stateabb",
            locationmode="USA-states",
            color="arithmeticmean",
            scope="usa",
            color_continuous_scale="Viridis",
            title=f"Average {filter_name()} Levels by State",
            labels={"arithmeticmean": ""},
        )

        return map_output

    @render_widget
    def plot_line():
        data = (
            df()
            .group_by(["statename", "year"])
            .agg(pl.col("arithmeticmean").mean())
            .sort(["statename", "year"])
        )

        line_output = px.line(
            data,
            x="year",
            y="arithmeticmean",
            color="statename",
            title=f"Average {filter_name()} Trends by State",
            labels={
                "year": "Year",
                "arithmeticmean": "Average Metric",
                "statename": "State",
            },
        ).update_layout(showlegend=False)

        return line_output

    @render_widget
    def plot_hist():
        hist_output = px.histogram(
            data_frame=df(), x="arithmeticmean", title=f"{filter_name()} Distribution"
        )

        return hist_output

    @render_widget
    def plot_scatter():
        scatter_output = px.scatter(
            data_frame=chat["df"](),
            x="90THPERCENTILE",
            y="10THPERCENTILE",
            title=f"{filter_name()} Percentile Comparison",
            labels={"arithmeticmean": "Measurement Value"},
            color="parametername",
        ).update_layout(showlegend=False)

        return scatter_output

    @render.data_frame
    def data_table():
        # Access filtered data via chat.df() reactive
        return chat["df"]()


# Create Shiny app
app = App(app_ui, server)
