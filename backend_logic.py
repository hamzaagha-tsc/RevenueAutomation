import pandas as pd
import os

def hms_to_sec(t):
    if pd.isna(t) or t == 0: return 0
    try:
        parts = str(t).strip().split(':')
        if len(parts) == 3: return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
        if len(parts) == 2: return int(parts[0])*60 + int(parts[1])
        return int(float(parts[0]))
    except: return 0

def sec_to_hms(s):
    h = int(s // 3600); m = int((s % 3600) // 60); s = int(s % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def run_attribution_process(orders_df, calls_df):
    # 1. Standardize Data
    orders_df['Order Time'] = pd.to_datetime(orders_df['Order Time'])
    calls_df['Call Time'] = pd.to_datetime(calls_df['Call Time'])
    calls_df['Secs'] = calls_df['User Talk Time'].apply(hms_to_sec)
    
    # 2. Define Windows
    feb_start = pd.Timestamp('2026-02-01')
    
    final_rows = []
    for _, order in orders_df.iterrows():
        o_id = order['Order ID']
        o_val = order['Order Value']
        o_phone = str(order['Order Phone']).strip()
        o_time = order['Order Time']
        
        # Filter calls for this customer before order time
        matches = calls_df[(calls_df['Phone Number'].astype(str).str.contains(o_phone)) & 
                           (calls_df['Call Time'] < o_time)]
        
        if matches.empty:
            final_rows.append({**order.to_dict(), 'Agent': 'Organic', 'Window A Time': '00:00:00', 'Window B Time': '00:00:00', 'Attributed Revenue': o_val})
            continue

        # Aggregate by Agent
        agent_data = []
        for agent, group in matches.groupby('User ID'):
            win_a_secs = group['Secs'].sum()
            win_b_secs = group[group['Call Time'] >= feb_start]['Secs'].sum()
            if win_a_secs >= 1: # Only consider agents with >= 1s connection
                agent_data.append({'Agent': agent, 'WinA': win_a_secs, 'WinB': win_b_secs})
        
        if not agent_data:
            final_rows.append({**order.to_dict(), 'Agent': 'Organic', 'Window A Time': '00:00:00', 'Window B Time': '00:00:00', 'Attributed Revenue': o_val})
            continue

        # 3. Apply Rules
        df_agents = pd.DataFrame(agent_data)
        
        if o_val < 100000:
            qual = df_agents[df_agents['WinA'] >= 60]
            if qual.empty:
                top = df_agents.sort_values('WinA', ascending=False).iloc[0]
                final_rows.append({**order.to_dict(), 'Agent': top['Agent'], 'Window A Time': sec_to_hms(top['WinA']), 'Window B Time': sec_to_hms(top['WinB']), 'Attributed Revenue': o_val})
            else:
                for _, ag in qual.iterrows():
                    final_rows.append({**order.to_dict(), 'Agent': ag['Agent'], 'Window A Time': sec_to_hms(ag['WinA']), 'Window B Time': sec_to_hms(ag['WinB']), 'Attributed Revenue': o_val})
        else:
            qual = df_agents[df_agents['WinA'] >= 180]
            if qual.empty:
                top = df_agents.sort_values('WinA', ascending=False).iloc[0]
                final_rows.append({**order.to_dict(), 'Agent': top['Agent'], 'Window A Time': sec_to_hms(top['WinA']), 'Window B Time': sec_to_hms(top['WinB']), 'Attributed Revenue': o_val})
            else:
                total_win_a = qual['WinA'].sum()
                for _, ag in qual.iterrows():
                    share = ag['WinA'] / total_win_a
                    final_rows.append({**order.to_dict(), 'Agent': ag['Agent'], 'Window A Time': sec_to_hms(ag['WinA']), 'Window B Time': sec_to_hms(ag['WinB']), 'Attributed Revenue': round(o_val * share, 2)})

    return pd.DataFrame(final_rows)
