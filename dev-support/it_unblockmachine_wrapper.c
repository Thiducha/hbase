/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <linux/limits.h>

extern char **environ;
extern int errno;

/**
 * Calls the script it_unblockmachine.sh. Supposed to be built, owned by root with the sticky bit.
 */
int main (int argc, char **argv) {
	char *path;
	uid_t euid;

	path = malloc(PATH_MAX * sizeof(char));

	if (path == NULL) {
		return EXIT_FAILURE;
	}

	euid = geteuid();

	setreuid(euid, euid);

	if (getcwd (path, PATH_MAX) == NULL) {
		return errno;
	}

	path = strcat(path, "/it_unblockmachine.sh");
	
	if (path == NULL) {
		return EXIT_FAILURE;
	}

	execve(path, NULL, environ);

	printf("Error : %d\n", errno);

	return errno;
}
