#!/bin/bash

# Usage check
if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 <num_rows> <times_per_command> [folders_to_skip]"
    echo "Example: $0 5 2 '0.1144,0.1156'"
    exit 1
fi

# Input parameters
num_rows=$1
times_per_command=$2
folders_to_skip=${3:-""}  # Optional third argument (comma-separated)

# Convert skip list into an array
IFS=',' read -ra skip_array <<< "$folders_to_skip"

# Output file
output_file="reconstruct_commands.sh"

# SLURM job header
cat > "$output_file" <<EOL
#!/bin/bash
#
#SBATCH -J   #name
#SBATCH -t 00:10:00  #time
#SBATCH -N 1         #node
#SBATCH --exclusive  
#
#SBATCH -e par-error.txt
#SBATCH -o par-output.txt

# load OpenFOAM (of10)
#module load OpenFOAM/10-opt-int32-hpc1-intel-2023a-eb
#
EOL

# Get all directories inside processors96
folders=($(ls -d processors96/*/ | awk -F'/' '{print $2}'))

# Filter out skipped folders
filtered_folders=()
for folder in "${folders[@]}"; do
    skip_flag=0
    for skip in "${skip_array[@]}"; do
        if [[ "$folder" == "$skip" ]]; then
            skip_flag=1
            break
        fi
    done
    if [[ $skip_flag -eq 0 ]]; then
        filtered_folders+=("$folder")
    fi
done

# Get the size of each folder (using 'du' to get the size in bytes)
folder_sizes=()
for folder in "${filtered_folders[@]}"; do
    size=$(du -sb "processors96/$folder" | cut -f1)  # Get the size in bytes
    folder_sizes+=("$size,$folder")
done

# Sort by size (in descending order)
IFS=$'\n' sorted_folders=($(sort -t',' -k1,1nr <<<"${folder_sizes[*]}"))
unset IFS

# Total folders available
total_folders=${#sorted_folders[@]}

# Ensure enough folders exist
total_needed=$(( num_rows * times_per_command ))
if (( total_needed > total_folders )); then
    echo "Warning: Not enough folders (${total_folders}) for $total_needed required times. Last row will have fewer entries."
fi

# Distribute folders in a greedy manner to balance the load across processors
processors=()
processor_sizes=()  # Array to store the total size of each processor
for ((i = 0; i < num_rows; i++)); do
    processors[i]=""
    processor_sizes[i]=0  # Initialize processor sizes to 0
done

# Greedy distribution: Distribute folders to the processor with the smallest total size
for ((i = 0; i < total_folders; i++)); do
    # Get the current folder size and name
    folder_size=$(echo "${sorted_folders[$i]}" | cut -d',' -f1)
    folder_name=$(echo "${sorted_folders[$i]}" | cut -d',' -f2)

    # Find processor with the least weight (size)
    min_index=0
    min_weight=${processor_sizes[0]}
    for ((j = 1; j < num_rows; j++)); do
        current_weight=${processor_sizes[j]}
        if ((current_weight < min_weight)); then
            min_weight=$current_weight
            min_index=$j
        fi
    done

    # Add the current folder to that processor
    processors[$min_index]+="${folder_name},"
    processor_sizes[$min_index]=$((processor_sizes[$min_index] + folder_size))  # Update processor size
done

# Generate commands for each processor
index=1
for ((row = 0; row < num_rows; row++)); do
    time_values=${processors[$row]}
    # Remove trailing comma
    time_values=${time_values%,}

    echo "mpirun -n 1 reconstructParMesh -time '$time_values' >> log.reconMesh$index &" >> "$output_file"
    ((index++))
done

index=1
for ((row = 0; row < num_rows; row++)); do
    time_values=${processors[$row]}
    # Remove trailing comma
    time_values=${time_values%,}

    echo "mpirun -n 1 reconstructPar -time '$time_values' >> log.recon$index &" >> "$output_file"
    ((index++))
done

# Add final wait at the end of the script
echo "wait" >> "$output_file"

# Make the script executable
chmod +x "$output_file"

## Print the total size associated with each processor (in bytes)
#echo "Total data size (in bytes) assigned to each processor:"
#for ((i = 0; i < num_rows; i++)); do
#    echo "Processor $((i + 1)): ${processor_sizes[$i]} bytes"
#done
#
echo "Generated $output_file with header, $num_rows rows, and $times_per_command time values per row, skipping folders: $folders_to_skip."
