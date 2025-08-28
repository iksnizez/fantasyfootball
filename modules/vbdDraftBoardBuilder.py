##### importing custom modules from the projects folder
import sys, os
from pathlib import Path
# Add project root to sys.path - search backwards through folders to find config.py
cwd = Path.cwd()
# Search upwards until a "config*" file is found
for parent in [cwd, *cwd.parents]:
    match = next(parent.glob('config*'), None)
    if match:
        PROJECT_ROOT = match.parent
        break
sys.path.append(str(PROJECT_ROOT))
import modules.helperModule as hf
##### -------------------------------------------------
from datetime import date
import pandas as pd


class vbdDraftBoard():

    def __init__(
        self,
        pick_cutoff = 120,
        season_cutoff = 2021,
        n_teams = 12,
        needed_starters = {'WR': 2, 'RB': 2, 'QB': 1, 'TE': 1, 'FLEX': 1}
    ):
        self.pick_cutoff = pick_cutoff
        self.season_cutoff = season_cutoff
        self.n_teams = n_teams

        needed_starters = {k: v * self.n_teams for k, v in needed_starters.items()}
        self.needed_starters = needed_starters  
        
        self.get_database_data()

    def get_database_data(self):
        # ==================================
        # GATHER DATA FROM MY DB
        # ==================================
        self.dfDrafts = hf.query_database(
            'SELECT * FROM ktbdrafts;'
        )
        self.dfProj = hf.query_database(
            """
            SELECT player.name, pos.pos, p.* 
            FROM projection p
            INNER JOIN player ON p.playerId = player.playerId
            LEFT JOIN pos ON player.posId = pos.posId
            WHERE season = 2025
            """
        )
        self.dfRank = hf.query_database(
            """
            SELECT player.name, pos.pos, o.outletName, a.analystName, r.* 
            FROM ranking r
            INNER JOIN player ON r.playerId = player.playerId
            LEFT JOIN pos ON player.posId = pos.posId
            LEFT JOIN outlet o ON r.outletId = o.outletId
            LEFT JOIN analyst a ON r.analystId = a.analystID
            WHERE season = 2025;
            """
        )
        self.dfAdp= hf.query_database(
            """
            SELECT player.name, pos.pos, o.outletName, a.* 
            FROM adp a
            INNER JOIN player ON a.playerId = player.playerId
            LEFT JOIN pos ON player.posId = pos.posId
            LEFT JOIN outlet o ON a.outletId = o.outletId
            WHERE date > '2025-04-30';
            """
        )
        self.dfPlayers2024 = hf.query_database(
            """
            SELECT kp.playerId, kp.playerName, pos.pos, kp.adp, kp.points, kp.pointsAvg,  
            kp.points/kp.pointsAvg gp, kp.positionRank, kp.overallRank
            FROM ktbplayers kp
            LEFT JOIN player p ON kp.playerId = p.espnId 
            LEFT JOIN pos ON p.posId = pos.posId
            WHERE season = 2024 AND points > 0;
            """
        ).drop_duplicates(keep='first')
        self.dfPlayers = hf.query_database(
            """
            SELECT player.playerId, player.name, pos.pos 
            FROM player
            LEFT JOIN pos ON pos.posId = player.posId
            WHERE player.playerId IS NOT NULL and player.name IS NOT NULL;
            """
        ).drop_duplicates(keep='first')
        self.dfProps = hf.query_database(
            """
            SELECT *
            FROM odds_season_totals
            WHERE date = (SELECT MAX(date) FROM odds_season_totals);
            """
        )

        return

    def get_replacement_player_score(self):

        needed_starters = self.needed_starters.copy()

        # ---- flex logic
        flex_eligible = self.dfPlayers2024.query(
            "(pos == 'WR' & positionRank > @needed_starters['WR']) | "
            "(pos == 'RB' & positionRank > @needed_starters['RB']) | "
            "(pos == 'TE' & positionRank > @needed_starters['TE'])"
        )
        flex_starters = (
            flex_eligible.nlargest(needed_starters['FLEX'], 'points')['pos']
            .value_counts()
            .to_dict()
        )
        for k, v in flex_starters.items():
            needed_starters[k] += v

        df_needed_starters = pd.DataFrame(
            list(needed_starters.items()), columns=['pos', 'needed_starters']
        )

        # ---- helper to compute averages
        def avg_counts(df, cutoff_query, col_name):
            counts = (
                df.query(cutoff_query)
                .groupby(['season', 'pos']).size()
                .groupby('pos').mean().astype(int)
                .rename(col_name)
            )
            return counts.reset_index()

        avg_all = avg_counts(self.dfDrafts, f'overallPick <= {self.pick_cutoff}', 'top_picks_all_years')
        avg_last3 = avg_counts(
            self.dfDrafts, f'overallPick <= {self.pick_cutoff} & season >= {self.season_cutoff}',
            'top_picks_last_3_years'
        )

        # ---- merge summary
        pos_keep = ['QB', 'RB', 'WR', 'TE']
        avg_pos_counts = (
            avg_all.merge(avg_last3, on='pos', how='outer')
            .merge(df_needed_starters, on='pos', how='outer')
            .fillna(0)
            .query('pos in @pos_keep')
            .sort_values('top_picks_all_years', ascending=False)
            .reset_index(drop=True)
        )

        # ---- replacement scores
        def lookup_score(row, col):
            rank = int(row[col])
            match = self.dfPlayers2024.query("pos == @row['pos'] & positionRank == @rank")['points']
            return match.iloc[0] if not match.empty else 0

        for col in avg_pos_counts.columns[1:]:
            avg_pos_counts[f'{col}_baseline_score'] = avg_pos_counts.apply(
                lambda r: lookup_score(r, col), axis=1
            )

        self.avg_pos_counts = avg_pos_counts.copy()
        return

    def get_stat_aggregations(self):
        # =======================
        #     PREP RANKINGS
        # =======================

        # updating rank groups to standardize position labels
        self.dfRank['rankGroup'] = self.dfRank['rankGroup'].replace({'DEF':'DST'})

        # outletName = fantasyPros ecr ranking is across all positions, have to remove it when aggregating 
        # unless its updated to be positional
        ## updating ECR TO Be positional ranking
        mask = self.dfRank['outletName'] == 'fantasyPros'

        self.dfRank.loc[mask, 'ranking'] = (
            self.dfRank[mask]
            .groupby('rankGroup')['ranking']
            .rank(method='dense', ascending=True)
        )

        df_agg_ranking = self.dfRank.groupby(
            ['name', 'playerId', 'rankGroup', 'date'] 
        ).agg(
            ranking_mean=('ranking', 'mean')
        ).reset_index()

        # filter to most recent ranking
        df_agg_ranking = df_agg_ranking.query('date == date.max()')

        # =======================
        #     PREP PROJECTIONS
        # =======================
        # filter to most recent projection
        df_agg_projection = self.dfProj.query('date == date.max() & fantasyPoints > 0')

        # calculate agg projections
        df_agg_projection = df_agg_projection.groupby(
            ['name', 'playerId', 'date'] 
        ).agg(
            projections_mean=('fantasyPoints', 'mean')
        ).reset_index()

        # =======================
        #     PREP ADP
        # =======================
        # filter to most recent projection
        df_agg_adp = self.dfAdp.query('date == date.max() & adp > 0')

        # calculate agg projections
        df_agg_adp = df_agg_adp.groupby(
            ['name', 'playerId', 'date']#, 'outletName'] 
        ).agg(
            adp_mean=('adp', 'mean'),
        ).reset_index()

        # =======================
        #     PREP PROPS
        # =======================
        # filter to most recent projection
        df_agg_props = self.dfProps.query('date == date.max()')

        # calculate agg projections
        df_agg_props  = (
            df_agg_props.pivot_table(
                index=['name', 'playerId'],
                columns='prop',
                values='current_line',
                aggfunc='first'   # handles duplicates safely
            )
            .add_suffix('_current_line')   # rename columns
            .reset_index()
        )
        df_agg_props = df_agg_props.drop(['name'], axis=1)


        # =======================
        #     COMBINE TO SINGLE RECORDS
        # =======================
        df_allstats_agg = df_agg_ranking[['playerId', 'ranking_mean']].merge(
            df_agg_projection[['playerId', 'projections_mean']],
            on='playerId',
            how='left'
        ).merge(
            df_agg_adp[['playerId', 'adp_mean']],
            on='playerId',
            how='left'
        ).merge(
            df_agg_props,
            on='playerId',
            how='left'
        )

        df_allstats_agg = df_allstats_agg.merge(
            self.dfPlayers[['playerId', 'name', 'pos']],
            on='playerId',
            how='left'
        )
        # =======================
        #     aDD ON VBD VALUES
        # =======================
        df_allstats_agg = df_allstats_agg.merge(
            self.avg_pos_counts[['pos', 'top_picks_last_3_years_baseline_score',	'needed_starters_baseline_score']],
            how='left',
            on='pos'
        )
        df_allstats_agg = df_allstats_agg.assign(
            VBD_historical = df_allstats_agg['projections_mean'] - df_allstats_agg['top_picks_last_3_years_baseline_score'],
            VBD_VOLS       = df_allstats_agg['projections_mean'] - df_allstats_agg['needed_starters_baseline_score']
        )

        col_order = [
            'name', 'pos', 'VBD_historical', 'VBD_VOLS', 'adp_mean', 'ranking_mean', 'projections_mean',  
            'total_passing_tds_current_line', 'total_passing_yds_current_line', 
            'total_receiving_tds_current_line', 'total_receiving_yds_current_line', 
            'total_rushing_tds_current_line', 'total_rushing_yds_current_line', 
            'top_picks_last_3_years_baseline_score', 'needed_starters_baseline_score', 
            'playerId'
        ]
        # round all numeric cols to 2 decimals
        df_allstats_agg[df_allstats_agg.select_dtypes(include='number').columns] = df_allstats_agg.select_dtypes(include='number').round(2)
        #sort
        df_allstats_agg = df_allstats_agg[col_order].sort_values('VBD_historical', ascending=False)
        #rename
        df_allstats_agg.rename(columns={
            'adp_mean':'adp', 'ranking_mean':'ranking', 'projections_mean':'proj',  
            'total_passing_tds_current_line':'pass_td', 'total_passing_yds_current_line':'pass_yd', 
            'total_receiving_tds_current_line':'rec_td', 'total_receiving_yds_current_line':'rec_yd', 
            'total_rushing_tds_current_line':'rush_td', 'total_rushing_yds_current_line':'rush_yd', 
            'top_picks_last_3_years_baseline_score':'baseline_scoreL3', 'needed_starters_baseline_score':'baseline_scoreReqStarters'
        }, inplace=True)
        self.df_allstats_agg = df_allstats_agg.copy()
        return
