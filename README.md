PROJECT TITLE: **Linux Inter-Process Communication Using Pipes and FIFOs**

OSSP Project
Team Member Ids:
2520030431,
2520030436,
2520030556

**Project Abstract:**
Inter-Process Communication (IPC) is an important concept in Operating Systems that allows different processes to exchange data and coordinate their activities. In Linux systems, processes normally have separate memory spaces, so a mechanism is required for safe and efficient communication between them. This project, “Linux Inter-Process Communication Using Pipes and FIFOs,” focuses on understanding and implementing two basic IPC mechanisms: pipes and named pipes (FIFOs). 

The project demonstrates how a Linux-based system can enable communication between processes using system calls and file descriptors. Pipes provide a simple way to transfer data between related processes, such as a parent and child process, while FIFOs allow communication between unrelated processes through a special named file. The project covers relevant Operating Systems and System Programming concepts such as process creation, process synchronization, file descriptors, system calls, reading and writing data, and process termination. 

The proposed solution will be implemented in C programming on Linux. Sample programs will be developed to create processes and demonstrate data transfer using both unnamed pipes and FIFOs. The communication flow and behavior of the processes will be observed and compared. 

The expected outcome of the project is a clear practical understanding of IPC in Linux and how pipes and FIFOs can be used for process communication. It will also help students gain hands-on experience with Linux system calls, process management, and basic system programming. 

**Setup Instructions:**
1. A computer running Linux or a Linux environment such as Ubuntu/WSL.
2. A C compiler, such as GCC.
3. A text editor or IDE, such as VS Code.
4. Basic knowledge of C programming and Linux commands.
The development environment will be set up by installing GCC and creating separate C programs for demonstrating communication using Pipes and FIFOs.

**Execution Instructions:**
Once the implementation is completed, the programs will be compiled and executed using the Linux terminal.
1. Write the C program for Pipe or FIFO communication.
2. Compile the program using GCC.
3. Execute the compiled program.
4. Observe the communication and data transfer between processes.
5. Compare the behavior of unnamed pipes and named pipes (FIFOs).

**Current Status: (Date: 28-08-2026):**
Project Initiation and Planning: The project is currently in its initial phase. The project title and abstract have been finalized, and the basic concepts and implementation approach have been identified. The coding and practical implementation have not yet started.
--Completed Work:
1. Project title finalized.
2. Abstract prepared.
3. Programming language and platform identified (C on Linux).
4. Basic project objectives defined.
----
5. Complete C implementation prepared in a single source file, `ipc_project.c`.
6. Sample log messages integrated into the program, eliminating the need for an external `sample.log` file.
7. Anonymous pipe communication implemented between the collector and filter processes.
8. Named FIFO communication implemented between the filter and analyzer processes.
9. Process creation, synchronization, and termination implemented using Linux system calls.
10. POSIX worker threads and mutex synchronization integrated into the analyzer.
11. Page-fault measurements and output report generation incorporated into the program.
12. Linux commands identified for observing process hierarchy, scheduling information, and thread activity.

**Updated Project Status:**

The project implementation has been prepared in C for Linux. The program integrates anonymous pipes and named FIFOs into a single log-processing workflow. It generates sample messages internally, filters valid and invalid records, analyzes accepted records using worker threads, and generates output files containing processed records and statistics.

The remaining work includes compiling and testing the implementation in the team's Linux or Ubuntu/WSL2 environment, verifying the generated outputs, demonstrating the actual Linux process scheduling, and preparing for the project review.

**Updated Execution Instructions:**

1. Open the Linux terminal in the directory containing `ipc_project.c`.
2. Compile the program using GCC:

   `gcc -Wall -Wextra -Wpedantic -pthread ipc_project.c -o ipc_project`

3. Execute the program:

   `./ipc_project`

4. Observe the collector, filter, and analyzer processes and the communication through the anonymous pipe and named FIFO.
5. Verify the generated files in the `output` directory: `processed.log` and `report.txt`.
6. Use Linux commands such as `ps`, `pstree`, `chrt`, and `top` to inspect process hierarchy, scheduling information, and thread activity.

The complete implementation uses a single C source file. An external sample log file, Makefile, additional C source files, and separate executables for individual process roles are not required.
