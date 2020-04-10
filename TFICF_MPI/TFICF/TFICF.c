#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<dirent.h>
#include<math.h>
#include "mpi.h"

/* This is the root process */
#define  ROOT   0

#define MAX_WORDS_IN_CORPUS 32
#define MAX_FILEPATH_LENGTH 16
#define MAX_WORD_LENGTH 16
#define MAX_DOCUMENT_NAME_LENGTH 8
#define MAX_STRING_LENGTH 64

typedef char word_document_str[MAX_STRING_LENGTH];

typedef struct o {
	char word[MAX_WORDS_IN_CORPUS];
	char document[MAX_DOCUMENT_NAME_LENGTH];
	int wordCount;
	int docSize;
	int numDocs;
	int numDocsWithWord;
} obj;

typedef struct w {
	char word[MAX_WORDS_IN_CORPUS];
	int numDocsWithWord;
	int currDoc;
} u_w;


// Global Variables
int numDocs;
int numDocsToProcess; // Number of documents to be Processed by that particular rank
int *indexesToProcess; // Indexes to Process

// Will hold all TFICF objects for asigned documents
obj TFICF[MAX_WORDS_IN_CORPUS];
int TF_idx = 0;

// Will hold all unique words in the corpus and the number of documents with that word
u_w unique_words[MAX_WORDS_IN_CORPUS];
int uw_idx = 0;

// Will hold the final strings that will be printed out
word_document_str strings[MAX_WORDS_IN_CORPUS];


// Function to Equal Distribution of Work
void scheduleIndexes(int rank, int numproc);

static int myCompare (const void * a, const void * b)
{
    return strcmp (a, b);
}

int main(int argc , char *argv[]){

	/* process information */
  int numproc, rank, len;

  /* current process hostname */
  char hostname[MPI_MAX_PROCESSOR_NAME];

	/* initialize MPI */
	MPI_Init(&argc, &argv);

	/* get the number of procs in the comm */
	MPI_Comm_size(MPI_COMM_WORLD, &numproc);

	/* get my rank in the comm */
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	/* get some information about the host I'm running on */
	MPI_Get_processor_name(hostname, &len);


	// Important Variables in use
	DIR* files;
	struct dirent* file;
	int i,j;
	int docSize, contains;
	char filename[MAX_FILEPATH_LENGTH], word[MAX_WORD_LENGTH], document[MAX_DOCUMENT_NAME_LENGTH];

	// Count numDocs at Root Node
	if (rank == ROOT){
		if((files = opendir("input")) == NULL){
			printf("Directory failed to open\n");
			exit(1);
		}
		while((file = readdir(files))!= NULL){
			// On linux/Unix we don't want current and parent directories
			if(!strcmp(file->d_name, "."))	 continue;
			if(!strcmp(file->d_name, "..")) continue;
			numDocs++;
		}
	}

	// Get numDocs Information from Root 0
	MPI_Bcast(&numDocs, 1, MPI_INT, ROOT, MPI_COMM_WORLD);

	if (rank != ROOT){
			scheduleIndexes(rank, numproc);

			// Loop through each assigned document and gather TFICF variables for each word
			for(i=0; i<numDocsToProcess; i++){
				sprintf(document, "doc%d", indexesToProcess[i]);
				sprintf(filename,"input/%s",document);
				FILE* fp = fopen(filename, "r");
				if(fp == NULL){
					printf("Error Opening File: %s\n", filename);
					exit(0);
				}

				// Get the document size
				docSize = 0;
				while((fscanf(fp,"%s",word)) != EOF)
					docSize++;

				// For each word in the document
				fseek(fp, 0, SEEK_SET);
				while((fscanf(fp,"%s", word)) != EOF){
					contains = 0;

					// If TFICF array already contains the word@document, just increment wordCount and break
					for(j=0; j<TF_idx; j++) {
						if(!strcmp(TFICF[j].word, word) && !strcmp(TFICF[j].document, document)){
							contains = 1;
							TFICF[j].wordCount++;
							break;
						}
					}

					//If TFICF array does not contain it, make a new one with wordCount=1
					if(!contains) {
						strcpy(TFICF[TF_idx].word, word);
						strcpy(TFICF[TF_idx].document, document);
						TFICF[TF_idx].wordCount = 1;
						TFICF[TF_idx].docSize = docSize;
						TFICF[TF_idx].numDocs = numDocs;
						TF_idx++;
					}

					contains = 0;
					// If unique_words array already contains the word, just increment numDocsWithWord
					for(j=0; j<uw_idx; j++) {
						if(!strcmp(unique_words[j].word, word)){
							contains = 1;
							if(unique_words[j].currDoc != indexesToProcess[i]) {
								unique_words[j].numDocsWithWord++;
								unique_words[j].currDoc = indexesToProcess[i];
							}
							break;
						}
					}

					// If unique_words array does not contain it, make a new one with numDocsWithWord=1
					if(!contains) {
						strcpy(unique_words[uw_idx].word, word);
						unique_words[uw_idx].numDocsWithWord = 1;
						unique_words[uw_idx].currDoc = indexesToProcess[i];
						uw_idx++;
					}
				}
				fclose(fp);
			}

			// Print TF job similar to HW4/HW5 (For debugging purposes) (At All other ranks other than 0)
			printf("-------------TF Job: Rank %d-------------\n", rank);
			for(j=0; j<TF_idx; j++)
				printf("%s@%s\t%d/%d\n", TFICF[j].word, TFICF[j].document, TFICF[j].wordCount, TFICF[j].docSize);

	}


	free(indexesToProcess);


	// // Use unique_words array to populate TFICF objects with: numDocsWithWord
	// for(i=0; i<TF_idx; i++) {
	// 	for(j=0; j<uw_idx; j++) {
	// 		if(!strcmp(TFICF[i].word, unique_words[j].word)) {
	// 			TFICF[i].numDocsWithWord = unique_words[j].numDocsWithWord;
	// 			break;
	// 		}
	// 	}
	// }
	//
	//
	// // Print ICF job similar to HW4/HW5 (For debugging purposes)
	// printf("------------ICF Job-------------\n");
	// for(j=0; j<TF_idx; j++)
	// 	printf("%s@%s\t%d/%d\n", TFICF[j].word, TFICF[j].document, TFICF[j].numDocs, TFICF[j].numDocsWithWord);
	//
	// // Calculates TFICF value and puts: "document@word\tTFICF" into strings array
	// for(j=0; j<TF_idx; j++) {
	// 	double TF = log( 1.0 * TFICF[j].wordCount / TFICF[j].docSize + 1 );
	// 	double ICF = log(1.0 * (TFICF[j].numDocs + 1) / (TFICF[j].numDocsWithWord + 1) );
	// 	double TFICF_value = TF * ICF;
	// 	sprintf(strings[j], "%s@%s\t%.16f", TFICF[j].document, TFICF[j].word, TFICF_value);
	// }
	//
	// // Sort strings and print to file
	// qsort(strings, TF_idx, sizeof(char)*MAX_STRING_LENGTH, myCompare);
	// FILE* fp = fopen("output.txt", "w");
	// if(fp == NULL){
	// 	printf("Error Opening File: output.txt\n");
	// 	exit(0);
	// }
	// for(i=0; i<TF_idx; i++)
	// 	fprintf(fp, "%s\n", strings[i]);
	// fclose(fp);

	return 0;
}

// Distribute the Work evenly among the ranks.
void scheduleIndexes(int rank, int numproc){
		 int i;
		 for (i=rank; i<=numDocs; i = i+numproc-1){
			 	 numDocsToProcess++;
		 }
		 indexesToProcess = (int*)malloc(sizeof(int)*numDocsToProcess);
		 int ind = 0;
		 for (i=rank; i<=numDocs; i = i+numproc-1){
			 	 indexesToProcess[ind] = i;
				 ind++;
		 }
}
