#!/bin/bash

v_file=$1
e_file=$2
v_name=$3
e_name=$4
g_name=$5
output=$6

cp ./templates/load_graph_temp.gsql load_graph.gsql
sed -i "s/VFILE/$v_file/g" load_graph.gsql
sed -i "s/EFILE/$e_file/g" load_graph.gsql
sed -i "s/VNAME/$v_name/g" load_graph.gsql
sed -i "s/ENAME/$e_name/g" load_graph.gsql
sed -i "s/GNAME/$g_name/g" load_graph.gsql

cp ./templates/conn_comp_temp.gsql conn_comp.gsql
sed -i "s/VNAME/$v_name/g" conn_comp.gsql
sed -i "s/ENAME/$e_name/g" conn_comp.gsql
sed -i "s/GNAME/$g_name/g" conn_comp.gsql

cp ./templates/louvain_temp.gsql louvain.gsql
sed -i "s/VNAME/$v_name/g" louvain.gsql
sed -i "s/ENAME/$e_name/g" louvain.gsql
sed -i "s/GNAME/$g_name/g" louvain.gsql
sed -i "s/OUTPUT/$output/g" louvain.gsql