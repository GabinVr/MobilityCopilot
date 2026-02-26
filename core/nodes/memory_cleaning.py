from langchain_core.messages import HumanMessage, RemoveMessage, ToolMessage, AIMessage
from core.state import CopilotState


def memory_cleaning_node(state: CopilotState) -> CopilotState:
    messages = state.get("messages", [])
    
    if len(messages) < 2:
        return {}
    
    messages_to_remove = []
    messages_to_keep = []

    for msg in messages[:-1]:
        is_trash = False

        if msg == messages[-1]:
            break

        if isinstance(msg, ToolMessage) and msg.id:
            is_trash = True
                
        elif isinstance(msg, AIMessage):
            if msg.tool_calls or (msg.content and "DATA GATHERING COMPLETE" in msg.content):
                if msg.id:
                    is_trash = True
                    
        elif isinstance(msg, HumanMessage) and msg.id:
            if "SQL execution successful" in msg.content:
                    is_trash = True

        if is_trash:
            messages_to_remove.append(RemoveMessage(id=msg.id))
        else:
            messages_to_keep.append(msg)

    MAX_MESSAGES = 10

    if len(messages_to_keep) > MAX_MESSAGES:
        excess_count = len(messages_to_keep) - MAX_MESSAGES

        for msg in messages_to_keep[:excess_count]:
            if msg.id:
                messages_to_remove.append(RemoveMessage(id=msg.id))
    

    return {"messages": messages_to_remove}