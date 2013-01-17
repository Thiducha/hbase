#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <linux/limits.h>

extern char **environ;
extern int errno;

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

	path = strcat(path, "./it_tests_unblockmachine.sh");
	
	if (path == NULL) {
		return EXIT_FAILURE;
	}

	execve(path, NULL, environ);

	printf("Error : %d\n", errno);

	return errno;
}
