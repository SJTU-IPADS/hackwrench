contention_choice=(0)
rl_data=`./eval/1_tpcc_tput_lat.py aws tpcc 0 hackwrench_fine_occ | grep -A 2 parameter | awk '/parameter/{getline a; print a}' `
sl_data=`./eval/1_tpcc_tput_lat.py aws tpcc 0 hackwrench_occ | grep -A 2 parameter | awk '/parameter/{getline a; print a}' `
rl_arr=(`echo $rl_data | tr "," "\n"`)
sl_arr=(`echo $sl_data | tr "," "\n"`)
index=1
echo -e 'ContentionFactor \t rl_tput \t sl_tput \t rl_remote_abort_ratio \t sl_remote_abort_ratio \t rl_local_abort_ratio \t sl_local_abort_ratio' > ../plot/data/record-vs-segment-lock-occ.data
for contention in ${contention_choice[@]};
do
    result=$contention" \t "${rl_arr[$index]}" \t "${sl_arr[$index]}" \t "${rl_arr[(($index+1))]}" \t "${sl_arr[(($index+1))]}" \t "${rl_arr[(($index+2))]}" \t "${sl_arr[(($index+2))]}
    ((index+=11));
    echo -e $result >> ../plot/data/record-vs-segment-lock-occ.data
done
