from langchain_core.messages import SystemMessage
from core.state import CopilotState
from utils.llm_provider import get_llm

from core.tools.tools_api_weather_now import geomet_mtl_weather_text_bundle
from core.tools.tools_api_histo import geomet_mtl_history_global_tool
from core.tools.sql_generator import sql_generator_tool
from core.tools.accidents_predictor import accidents_predictor_tool
import time

def data_agent_node(state: CopilotState) -> CopilotState:

    llm = get_llm()

    question = state.get("question", "No question found.")
    messages = state.get("messages", [])

    tools = [geomet_mtl_weather_text_bundle, geomet_mtl_history_global_tool, sql_generator_tool, accidents_predictor_tool]
    llm_with_tools = llm.bind_tools(tools, parallel_tool_calls=False)

    today = time.strftime("%Y-%m-%d")

    db_schema = state.get("database_schema", "No database schema found.")
    querying_tips = state.get("querying_tips", "No querying tips found.")
    table_descriptions = state.get("table_descriptions", "No table descriptions found.")

    system_instruction = (
		"You are the Data Agent for Montreal Mobility.\n"
        "Your PRIMARY action is to find raw data using your available tools.\n\n"

        "Assume that today is " + today + "so if the user asks for data with links to the current date, use this date in your queries.\n\n"

        " 🚨 PREDICTIVE MODELING (FUTURE ACCIDENTS) 🚨"
        " If the user asks to predict, forecast, or estimate numer of accidents or collisions for today or tomorrow :"
        " STEP 1: You MUST first call the `geomet_mtl_weather_text_bundle` tool to get the weather forecast for that date."
        " STEP 2: Read the temperature and precipitation from the weather tool's response. if you don't find some information, assume it's similar to today's weather."
        " STEP 3: Then you MUST use the `accidents_predictor_tool` using the EXACT data you just got from the weather forecast. you can't let one of the features empty"
        " STEP 4: Output 'DATA GATHERING COMPLETE' with the predicted number of accidents."


        "🚨 MANDATORY ACTION MAPPING 🚨\n"
        "1. SQL ONLY: IF the subject is 'collisions', 'accidents', 'potholes', 'requests 311' or 'sectors', YOU MUST USE THE 'generate_and_validate_sql' TOOL.\n"
        "2. WEATHER ONLY: IF the subject is only 'temperature', 'snow quantity', or 'rain', use the Weather Tools.\n"
        "3. HYBRID CORRELATIONS & WEATHER IMPACTS (CRITICAL): How to handle weather depends entirely on the table:\n"
        "   - FOR COLLISIONS: The 'collisions_routieres' table ALREADY has a weather column (`CD_COND_METEO`). DO NOT compare months! Instead, use ONE SQL QUERY with CASE WHEN to compare clear weather ('11') vs bad weather ('14','15','17','18','19') in specific sectors. \n"
        "     Example: `SELECT ACCDN_PRES_DE, SUM(CASE WHEN CD_COND_METEO = '11' THEN 1 ELSE 0 END) as clear_weather, SUM(CASE WHEN CD_COND_METEO IN ('14','15','17','18','19') THEN 1 ELSE 0 END) as bad_weather FROM collisions_routieres WHERE ACCDN_PRES_DE IS NOT NULL GROUP BY ACCDN_PRES_DE ORDER BY bad_weather DESC LIMIT 5;`\n"
        "   - FOR 311 REQUESTS: The 'requetes311' table has NO weather column. Therefore, you MUST use ONE SINGLE SQL QUERY using CASE WHEN to compare a cold month (e.g., '2023-01') and a warm month (e.g., '2023-07') by grouping by NATURE or ARRONDISSEMENT.\n"        
        "   -If you have to compare cold vs warm months, you can assume that winter months are December, January, February and summer months are June, July, August. So compare 3 month winter period vs 3 month summer period "
		
		"🚨 THE WEATHER ALGORITHM (ONLY FOR FINDING EXTREMES) 🚨\n"
        "1. CURRENT WEATHER: For today's data or forecasts, use 'geomet_mtl_weather_text_bundle'.\n"
        "2. FINDING EXTREMES: ONLY if the user explicitly asks for an extreme (e.g., 'the coldest day', 'the biggest snowstorm'), use the 'geomet_mtl_history_global_tool' with this STRICT sequential process:\n"
        "   STEP 1: Call frequency='year' for the target period. Identify the extreme YEAR.\n"
        "   STEP 2: Call frequency='month' ONLY for that extreme YEAR. Identify the extreme MONTH.\n"
        "   STEP 3: Call frequency='week' ONLY for that extreme MONTH. Identify the extreme WEEK.\n"
        "   STEP 4: Call frequency='day' ONLY for the 7 days of that extreme WEEK. Get the exact DAY.\n"
        "   STEP 5: Data Gathering Complete.\n"
        " If the question is NOT about finding a specific extreme day, DO NOT use this 5-step process.\n\n"        

		"⛔ ABSOLUTE PROHIBITIONS (DO NOT VIOLATE) ⛔\n"
        "- YOU ARE STRICTLY FORBIDDEN from using frequency='day' for more than 7 days in a single tool call.\n"
        "- YOU ARE STRICTLY FORBIDDEN from doing exhaustive verification. If STEP 3 says 'Week 3' is the coldest, DO NOT check Week 1, 2, or 4.\n"
        "- DO NOT loop endlessly. Once you find the exact DAY in STEP 4, YOU MUST STOP IMMEDIATELY and proceed to the Handoff.\n\n"
        
        "🚨 DATABASE INFORMATIONS 🚨\n"
		"<database_context>\n"
        f"{db_schema}\n\n"
        f"{querying_tips}\n\n"
        f"{table_descriptions}\n\n"
		"</database_context>\n\n"
        
        "🚨 EXITING THE LOOP & HANDOFF (CRITICAL) 🚨\n"
        "You are ONLY a data retriever. YOU ARE FORBIDDEN from writing the final response for the user. The Synthesis node will do that.\n"
        "Once you have successfully gathered necessary data, you MUST stop.\n"
        "Your final message MUST start EXACTLY with 'DATA GATHERING COMPLETE:' followed ONLY by a raw bulleted list of facts or JSON.\n"
        "DO NOT write full sentences. DO NOT say 'Le jour avec le plus de...'. DO NOT be conversational.\n\n"
		f"The question to answer is: {question}\n\n"
		)
    
    response = llm_with_tools.invoke([SystemMessage(content=system_instruction)] + messages)
    
    return {"messages": [response]}