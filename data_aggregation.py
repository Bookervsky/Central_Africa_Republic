import geopandas as gpd
import os
from typing import List


class MyLayer:
    def __init__(self, data_dir: str, prefecture_file: str, years: List):
        self.data_dir = data_dir
        self.prefecture_file = prefecture_file
        self.years = years
        self.prefecture = None
        self.data_group = {}

    def run_workflow(self, subcategory=False):
        """
        Run the complete workflow: read shapefiles and aggregate data to prefecture level.
        :return:
        """
        self.read_prefecture()
        default_prefecture = self.prefecture.copy()
        for year in self.years:
            self.read_shp(year)
            self.aggregate_data(subcategory=subcategory)
            self.export_data(year)
            self.prefecture = default_prefecture.copy()
            self.prefecture.to_crs("EPSG:3857", inplace=True)
            self.data_group = {}

    def read_prefecture(self):
        """
        Read the prefecture shapefile and set its CRS to EPSG:3857.
        :return:
        """
        self.prefecture = gpd.read_file(
            self.data_dir + "/Prefectures/" + self.prefecture_file
        )
        self.prefecture = self.prefecture[
            [
                "OBJECTID",
                "admin0Name",
                "admin1Name",
                "Shape_Leng",
                "Shape_Area",
                "geometry",
            ]
        ]
        self.prefecture.rename(
            columns={"admin0Name": "country", "admin1Name": "prefecture"}, inplace=True
        )
        self.prefecture.to_crs("EPSG:3857", inplace=True)

    def read_shp(self, year):
        """
        Read all shapefiles in the 'data/POI/year/' directory and store them in a dictionary.
        :return:
        """
        data_dir = self.data_dir + "/POI/" + str(year) + "/"
        for shp_file in os.listdir(data_dir):
            if shp_file.endswith(".shp"):
                data_name = shp_file.split("_")[2]
                gdf = gpd.read_file(os.path.join(data_dir, shp_file))
                gdf.to_crs("EPSG:3857", inplace=True)
                self.data_group[data_name] = gdf

    def aggregate_data(self, subcategory=False):
        """
        Aggregate all data in self.data_group to prefecture level, based on their geometry types
        :return:
        """
        for data_name, gdf in self.data_group.items():
            geom_type = gdf.geometry.geom_type.unique()
            if len(geom_type) == 0:
                continue
            elif set(geom_type).issubset({"Point", "MultiPoint"}):
                self.point_number_in_polygon(
                    gdf, data_name + "_count", subcategory=subcategory
                )
            elif set(geom_type).issubset({"LineString", "MultiLineString"}):
                self.line_length_in_polygon(
                    gdf, data_name + "_length", subcategory=subcategory
                )
            elif set(geom_type).issubset({"Polygon", "MultiPolygon"}):
                self.polygon_area_in_polygon(
                    gdf, data_name + "_area", subcategory=subcategory
                )
                # self.polygon_number_in_polygon(gdf, data_name + '_count')

    def point_number_in_polygon(self, points, point_count, subcategory=False):
        """
        Aggregate POINT type data, such as places
        Calculate the number of poi_name within each prefecture.
        :param prefecture:
        :param poi: (POINT type GeoDataFrame)
        :param poi_name:
        :return:
        prefecture with new column for total number of poi
        """
        # Total number of points within each prefecture
        joined = gpd.sjoin(points, self.prefecture, how="inner", predicate="within")
        counts = joined.groupby("OBJECTID").size().rename(point_count)
        self.prefecture[point_count] = (
            self.prefecture["OBJECTID"].map(counts).fillna(0).astype(int)
        )

        # Subtotal number of points within each prefecture by category
        if subcategory:
            subcounts = (
                joined.groupby(["OBJECTID", "fclass"])
                .size()
                .rename(point_count)
                .reset_index()
            )
            for _, row in subcounts.iterrows():
                col_name = f"{row['fclass']}_{point_count}"
                if col_name not in self.prefecture.columns:
                    self.prefecture[col_name] = 0
                self.prefecture.loc[
                    self.prefecture["OBJECTID"] == row["OBJECTID"], col_name
                ] = row[point_count]

    def line_length_in_polygon(self, lines, line_name, subcategory=False):
        """
        For LINE type data, such as roads
        Calculate the total length of poi_name within each prefecture.
        :param lines: LINE type GeoDataFrame
        :param line_name: line length column name
        :return:
        prefecture with new column for total line length
        """
        # Total length of lines within each prefecture
        intersections = gpd.overlay(lines, self.prefecture, how="intersection")
        intersections["length"] = intersections.geometry.length
        length_sum = (
            intersections.groupby("OBJECTID")["length"]
            .sum()
            .rename(line_name)
            .reset_index()
        )
        self.prefecture[line_name] = length_sum[line_name]
        self.prefecture[line_name] = self.prefecture[line_name].fillna(0)

        # Subtotal length of lines within each prefecture by category
        if subcategory:
            sublengths = (
                intersections.groupby(["OBJECTID", "fclass"])["length"]
                .sum()
                .rename(line_name)
                .reset_index()
            )
            for _, row in sublengths.iterrows():
                col_name = f"{row['fclass']}_{line_name}"
                if col_name not in self.prefecture.columns:
                    self.prefecture[col_name] = 0
                self.prefecture.loc[
                    self.prefecture["OBJECTID"] == row["OBJECTID"], col_name
                ] = row[line_name]

    def polygon_area_in_polygon(self, polygons, polygon_name, subcategory=False):
        """
        For pois of POLYGON type, such as buildings
        Calculate the total area of poi_name within each prefecture.
        :param polygons: POLYGON type GeoDataFrame
        :param polygon_name: polygon area column name
        :return:
        prefecture with new column for total area of polygons
        """
        # Total area of polygons within each prefecture
        polygons["area"] = polygons.geometry.area
        joined = gpd.sjoin(polygons, self.prefecture, how="left", predicate="within")
        area_sum = (
            joined.groupby("OBJECTID")["area"].sum().rename(polygon_name).reset_index()
        )
        self.prefecture[polygon_name] = area_sum[polygon_name]
        self.prefecture[polygon_name] = self.prefecture[polygon_name].fillna(0)

        # Subtotal area of polygons within each prefecture by category
        if subcategory:
            subareas = (
                joined.groupby(["OBJECTID", "fclass"])["area"]
                .sum()
                .rename(polygon_name)
                .reset_index()
            )
            for _, row in subareas.iterrows():
                col_name = f"{row['fclass']}_{polygon_name}"
                if col_name not in self.prefecture.columns:
                    self.prefecture[col_name] = 0
                self.prefecture.loc[
                    self.prefecture["OBJECTID"] == row["OBJECTID"], col_name
                ] = row[polygon_name]

    def polygon_number_in_polygon(self, polygons, polygon_name):
        """
        For POLYGON type data, such as buildings
        Calculate the number of poi_name within each prefecture.
        :param polygons: POLYGON type GeoDataFrame
        :param polygon_name: polygon count column name
        :return:
        prefecture with new column for number of polygons
        """
        joined = gpd.sjoin(polygons, self.prefecture, how="inner", predicate="within")
        counts = joined.groupby("OBJECTID").size().rename(polygon_name)
        self.prefecture[polygon_name] = (
            self.prefecture["OBJECTID"].map(counts).fillna(0).astype(int)
        )

    def export_data(self, year):
        output_file = (
            self.data_dir
            + "/Aggregated_prefecture/geojson/"
            + f"Aggregated_Prefecture_{year}.geojson"
        )
        self.prefecture.to_crs("EPSG:4326", inplace=True)
        self.prefecture.to_file(output_file, driver="GeoJSON")
        # export csv
        self.prefecture["geometry_wkt"] = self.prefecture["geometry"].apply(lambda geom: geom.wkt)
        self.prefecture.drop(columns=["geometry"]).to_csv(
            self.data_dir
            + "/Aggregated_prefecture/csv/"
            + f"Aggregated_Prefecture_{year}.csv",
            index=False
        )

if __name__ == "__main__":
    years = [2018, 2019, 2020]
    central_african_republic = MyLayer(
        data_dir="data",
        prefecture_file="caf_admbnda_adm1_200k_sigcaf_reach_itos_Ocha.shp",
        years=years,
    )
    central_african_republic.run_workflow(subcategory=True)
