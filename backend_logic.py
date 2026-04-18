import pandas as pd

def to_sec(t):
    """Converts HH:MM:SS or MM:SS to total seconds."""
    if pd.isna(t) or t == 0: return 0
    try:
        parts = str(t).strip().split(':')
        if len(parts) == 3: return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
        if len(parts) == 2: return int(parts[0])*60 + int(parts[1])
        return int(parts[0])
    except: return 0

def run_attribution(df):
    # Ensure date/numeric types
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce')
    df['WindowA_Sec'] = df['Window A Time'].apply(to_sec)
    
    # We group by the Order ID (Name) to process each order's set of agents
    order_groups = df.groupby('Name')
    final_results = []

    for name, group in order_groups:
        order_val = group['Total'].iloc[0]
        
        # Identify Agent Set (Connected with lead >= 1 second in Window A)
        agent_set = group[group['WindowA_Sec'] >= 1].copy()
        
        if agent_set.empty:
            # If no one even hit 1 second, it remains Organic
            row = group.iloc[0].to_dict()
            row['Attributed Revenue'] = order_val
            row['Agent Name'] = 'Organic'
            final_results.append(row)
            continue

        # --- PATH A: < 1,00,000 (Duplication) ---
        if order_val < 100000:
            qualified = agent_set[agent_set['WindowA_Sec'] >= 60]
            
            if not qualified.empty:
                for _, ag in qualified.iterrows():
                    res = ag.to_dict()
                    res['Attributed Revenue'] = order_val
                    final_results.append(res)
            else:
                # Fallback: Highest talker in Window A gets 100%
                top_ag = agent_set.sort_values(by='WindowA_Sec', ascending=False).iloc[0].to_dict()
                top_ag['Attributed Revenue'] = order_val
                final_results.append(top_ag)

        # --- PATH B: >= 1,00,000 (Proportionate Split) ---
        else:
            qualified = agent_set[agent_set['WindowA_Sec'] >= 180]
            
            if not qualified.empty:
                total_q_sec = qualified['WindowA_Sec'].sum()
                for _, ag in qualified.iterrows():
                    res = ag.to_dict()
                    share = ag['WindowA_Sec'] / total_q_sec
                    res['Attributed Revenue'] = round(order_val * share, 2)
                    final_results.append(res)
            else:
                # Fallback: Highest talker in Window A gets 100%
                top_ag = agent_set.sort_values(by='WindowA_Sec', ascending=False).iloc[0].to_dict()
                top_ag['Attributed Revenue'] = order_val
                final_results.append(top_ag)

    return pd.DataFrame(final_results)
