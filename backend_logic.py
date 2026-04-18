import pandas as pd

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
    # 1. Pre-process and Standardize
    orders_df['Order Time'] = pd.to_datetime(orders_df['Order Time'])
    calls_df['Call Time'] = pd.to_datetime(calls_df['Call Time'])
    calls_df['Secs'] = calls_df['User Talk Time'].apply(hms_to_sec)
    feb_start = pd.Timestamp('2026-02-01')
    
    # Clean phone numbers to ensure a perfect match
    orders_df['JoinPhone'] = orders_df['Order Phone'].astype(str).str.strip()
    calls_df['JoinPhone'] = calls_df['Phone Number'].astype(str).str.strip()

    # 2. VECTORIZED MERGE: Match calls to orders by phone number
    # We only keep calls from phone numbers that actually placed an order
    merged = pd.merge(
        orders_df, 
        calls_df[['JoinPhone', 'Call Time', 'User ID', 'Secs']], 
        on='JoinPhone', 
        how='left'
    )

    # 3. BULK FILTER: Keep only calls that happened before the order
    # This replaces the loop entirely
    merged = merged[merged['Call Time'] < merged['Order Time']].copy()
    
    # 4. AGGREGATE: Calculate Window A and Window B in one go
    # Window A is total Secs. Window B is Secs where Call Time >= Feb 1st
    merged['WinB_Secs'] = merged.apply(lambda x: x['Secs'] if x['Call Time'] >= feb_start else 0, axis=1)
    
    grouped = merged.groupby(['Order ID', 'User ID']).agg({
        'Secs': 'sum',
        'WinB_Secs': 'sum'
    }).reset_index()
    
    # 5. FINAL ATTRIBUTION LOGIC
    final_rows = []
    # Loop only through orders now (much faster since the math is done)
    for o_id, o_group in orders_df.groupby('Order ID'):
        o_val = o_group['Order Value'].iloc[0]
        o_dict = o_group.iloc[0].to_dict()
        
        # Get pre-calculated agent stats for this order
        agent_stats = grouped[grouped['Order ID'] == o_id].copy()
        
        if agent_stats.empty:
            o_dict.update({'Agent': 'Organic', 'Window A Time': '00:00:00', 'Window B Time': '00:00:00', 'Attributed Revenue': o_val})
            final_rows.append(o_dict)
            continue

        # Rule Selection
        if o_val < 100000:
            qual = agent_stats[agent_stats['Secs'] >= 60]
            if qual.empty:
                top = agent_stats.sort_values('Secs', ascending=False).iloc[0]
                o_dict.update({'Agent': top['User ID'], 'Window A Time': sec_to_hms(top['Secs']), 'Window B Time': sec_to_hms(top['WinB_Secs']), 'Attributed Revenue': o_val})
                final_rows.append(o_dict)
            else:
                for _, ag in qual.iterrows():
                    res = o_dict.copy()
                    res.update({'Agent': ag['User ID'], 'Window A Time': sec_to_hms(ag['Secs']), 'Window B Time': sec_to_hms(ag['WinB_Secs']), 'Attributed Revenue': o_val})
                    final_rows.append(res)
        else:
            qual = agent_stats[agent_stats['Secs'] >= 180]
            if qual.empty:
                top = agent_stats.sort_values('Secs', ascending=False).iloc[0]
                o_dict.update({'Agent': top['User ID'], 'Window A Time': sec_to_hms(top['Secs']), 'Window B Time': sec_to_hms(top['WinB_Secs']), 'Attributed Revenue': o_val})
                final_rows.append(o_dict)
            else:
                total_a = qual['Secs'].sum()
                for _, ag in qual.iterrows():
                    res = o_dict.copy()
                    share = ag['Secs'] / total_a
                    res.update({'Agent': ag['User ID'], 'Window A Time': sec_to_hms(ag['Secs']), 'Window B Time': sec_to_hms(ag['WinB_Secs']), 'Attributed Revenue': round(o_val * share, 2)})
                    final_rows.append(res)

    return pd.DataFrame(final_rows)
