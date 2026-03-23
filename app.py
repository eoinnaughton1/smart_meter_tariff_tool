import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import calendar

from profiling import clean_esb_file
from matching import match_tariffs, cost_breakdown
from profiles import generate_profile


# confogure the page
st.set_page_config(
    page_title="Smart Meter Tariff Tool",
    page_icon="",
    layout="wide"
)

st.title("Tariff Recommendation Tool")

st.markdown("""
Upload your smart meter data to:
- Analyse your electricity usage profile  
- Compare available market tariffs  
- Identify the most cost-effective plan  
""")

#allows user option to upload their date or enter estimate
st.sidebar.header("Tool Settings")

mode = st.sidebar.radio(
    "Select mode:",
    ["Upload Smart Meter File", "Enter Annual Usage Only"]
)

st.divider()

# cache and read in the tariff plans from csv
@st.cache_data
def load_tariffs():
    import glob
    csv_files = glob.glob("scraping/data/tariffs/clean/*_auto.csv")
    if not csv_files:
        st.error("No tariff data found. Run the scraper first.")
        return pd.DataFrame()
    dfs = [pd.read_csv(f) for f in csv_files]
    return pd.concat(dfs, ignore_index=True)

def display_matching_results(clean_df):
#run tariff comparison from users data
    tariffs = load_tariffs()
    results_df = match_tariffs(clean_df, tariffs)

    # Cdisplay columns
    display_df = results_df[[
        "supplier", "plan_name", "meter_type", "structure",
        "estimated_cost_eur", "cost_per_kwh_cent",
        "energy_cost_eur", "discount_saving_eur",
        "standing_charge_eur"
    ]].copy()
    display_df.columns = [
        "Supplier", "Plan", "Meter Type", "Structure",
        "Total Cost (€)", "Effective c/kWh",
        "Energy (€)", "Discount (€)",
        "Standing Charge (€)"
    ]
    #display ranked results
    st.subheader("Ranked Tariff Results")
    st.dataframe(
        display_df.style.format({
            "Total Cost (€)": "€{:,.2f}",
            "Effective c/kWh": "{:.2f}c",
            "Energy (€)": "€{:,.2f}",
            "Discount (€)": "€{:,.2f}",
            "Standing Charge (€)": "€{:,.2f}",
        }),
        use_container_width=True,
    )

    if len(results_df) < 2:
        return

    best = results_df.iloc[0]
    # find first plan and then compare against next plan
    second = results_df[
        results_df["estimated_cost_eur"] > best["estimated_cost_eur"]
    ]
    if len(second) > 0:
        savings = second.iloc[0]["estimated_cost_eur"] - best["estimated_cost_eur"]
    else:
        savings = 0

    st.success(
        f"""
        **Best Plan:** {best['plan_name']} ({best['structure']})  
        **Estimated Annual Cost:** €{best['estimated_cost_eur']:.2f}  
        **Savings vs Next Best:** €{savings:.2f}
        """
    )
    #show best tariff details
    best_row = tariffs[tariffs["plan_name"] == best["plan_name"]].iloc[0]
    breakdown = cost_breakdown(clean_df, best_row)

    total = breakdown.get("total", 0)
    gross_energy = sum(v for k, v in breakdown.items() if k.startswith("energy"))
    discount = abs(breakdown.get("discount saving", 0))
    net_energy = gross_energy - discount
    standing = breakdown.get("standing charge", 0)

    # summary of the metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Energy Cost", f"€{gross_energy:,.2f}")
    with col2:
        st.metric("Discount Saving", f"-€{discount:,.2f}")
    with col3:
        st.metric("Standing Charge", f"€{standing:,.2f}")
    with col4:
        st.metric("Total Annual Cost", f"€{total:,.2f}")

    #charts beside each other
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        #pie chart
        pie_labels = ["Energy (after discount)", "Standing Charge"]
        pie_values = [net_energy, standing]
        fig_pie = px.pie(
            names=pie_labels,
            values=pie_values,
            title="Cost Split",
            color_discrete_sequence=["#3498db", "#e67e22"],
            hole=0.4,
        )
        fig_pie.update_traces(
            textinfo="label+percent",
            textposition="outside",
            pull=[0.02, 0.02],
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with chart_col2:
        #show the time periods that effect energy bill most
        energy_items = {
            k: v for k, v in breakdown.items()
            if k.startswith("energy")
        }
        fig_bar = px.bar(
            x=list(energy_items.keys()),
            y=list(energy_items.values()),
            title="Energy Cost by Period (before discount)",
            labels={"x": "", "y": "€"},
            color_discrete_sequence=["#3498db"],
        )
        fig_bar.update_traces(
            text=[f"€{v:,.2f}" for v in energy_items.values()],
            textposition="outside",
        )
        fig_bar.update_layout(showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    #cheapest plan at top
    top5 = results_df.head(5).copy()
    top5 = top5.iloc[::-1]

    fig2 = go.Figure()

    discounted_energy = top5["energy_cost_eur"] + top5["discount_saving_eur"]
    fig2.add_trace(go.Bar(
        y=top5["plan_name"],
        x=discounted_energy,
        name="Energy (after discount)",
        orientation="h",
        marker_color="#3498db",
        text=[f"€{v:,.0f}" for v in discounted_energy],
        textposition="inside",
    ))
    fig2.add_trace(go.Bar(
        y=top5["plan_name"],
        x=top5["standing_charge_eur"],
        name="Standing Charge",
        orientation="h",
        marker_color="#e67e22",
        text=[f"€{v:,.0f}" for v in top5["standing_charge_eur"]],
        textposition="inside",
    ))

    fig2.update_layout(
        barmode="stack",
        title="Top 5 Plans — Cost Comparison",
        xaxis_title="Estimated Annual Cost (€)",
        yaxis_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )

    st.plotly_chart(fig2, use_container_width=True)

#uploading of files
if mode == "Upload Smart Meter File":

    uploaded_file = st.file_uploader("Upload your smart meter CSV", type=["csv"])

    if uploaded_file is not None:

        clean_df = clean_esb_file(uploaded_file)

        st.success("File uploaded")

        #summary of users consmption
        col1, col2 = st.columns(2)

        with col1:
            st.metric("Total Annual Consumption (kWh)",
                      f"{clean_df['kWh'].sum():,.0f}")

        with col2:
            st.metric("Date Range",
                      f"{clean_df['date'].min()} → {clean_df['date'].max()}")

        st.divider()

        #monthly totals display
        monthly = (
            clean_df
            .groupby(clean_df["timestamp_local"].dt.to_period("M"))["kWh"]
            .sum()
            .to_timestamp()
        )

        month_labels = [calendar.month_abbr[m] for m in monthly.index.month]

        monthly_df = pd.DataFrame({
            "Month": month_labels,
            "kWh": monthly.values
        })

        fig_month = px.bar(
            monthly_df,
            x="Month",
            y="kWh",
            title="Monthly Energy Usage (kWh)",
            text_auto=".0f"
        )

        st.plotly_chart(fig_month, use_container_width=True)

        #typical day displau by averaging
        slots = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]

        clean_df["slot"] = clean_df["timestamp_local"].dt.strftime("%H:%M")
        typical = clean_df.groupby("slot")["kWh"].mean().reindex(slots)


        def classify_tou(slot):
            hh = int(slot.split(":")[0])
            minutes = hh * 60

            if minutes >= 23 * 60 or minutes < 8 * 60:
                return "Off Peak"
            if 17 * 60 <= minutes < 19 * 60:
                return "On Peak"
            return "Mid Peak"

        tou = [classify_tou(s) for s in typical.index]

        typ_df = pd.DataFrame({
            "Time": typical.index,
            "kWh": typical.values,
            "TOU": tou
        })

        fig_typ = px.bar(
            typ_df,
            x="Time",
            y="kWh",
            color="TOU",
            color_discrete_map={
                "Off Peak": "green",
                "Mid Peak": "gold",
                "On Peak": "red"
            },
            title="Typical Day Profile (Average kWh per 30-min)"
        )

        #error with ordering fix i.e. makes them stay in time order
        fig_typ.update_layout(
            xaxis=dict(
                categoryorder="array",
                categoryarray=slots
            )
        )

        st.plotly_chart(fig_typ, use_container_width=True)

        #graph o weekend vs weekday
        weekday = clean_df[~clean_df["is_weekend"]].groupby("slot")["kWh"].mean().reindex(slots)
        weekend = clean_df[clean_df["is_weekend"]].groupby("slot")["kWh"].mean().reindex(slots)

        compare_df = pd.DataFrame({
            "Time": slots,
            "Weekday": weekday.values,
            "Weekend": weekend.values
        })

        compare_melt = compare_df.melt(id_vars="Time", var_name="Type", value_name="kWh")

        fig_compare = px.line(
            compare_melt,
            x="Time",
            y="kWh",
            color="Type",
            color_discrete_map={
                "Weekday": "#1f77b4",
                "Weekend": "#ff7f0e"
            },
            title="Weekday vs Weekend Usage Profile"
        )

        fig_compare.update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=slots[::4],
                ticktext=slots[::4]
            )
        )

        st.plotly_chart(fig_compare, use_container_width=True)

        st.divider()


        display_matching_results(clean_df)


#mode for entering usage manually
elif mode == "Enter Annual Usage Only":

    annual_kwh = st.number_input(
        "Enter total annual electricity usage (kWh)",
        min_value=1000.0,
        step=100.0
    )

    profile_type = st.selectbox(
        "Select Usage Profile",
        ["Typical Household", "Night-Heavy (EV)", "Peak-Heavy"]
    )

    if annual_kwh > 0:

        synthetic_df = generate_profile(profile_type, annual_kwh)

        st.success("Profile generated")

        #keep the visuals the same
        display_matching_results(synthetic_df)