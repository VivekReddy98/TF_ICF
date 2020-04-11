/*
Single Author Info
vkarri Vivek Reddy Karri
*/

#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<dirent.h>
#include<math.h>
#include<stddef.h>
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
	char word[32];
	char document[8];
	int wordCount;
	int docSize;
	int numDocs;
	int numDocsWithWord;
} obj;

typedef struct w {
	char word[32];
	int numDocsWithWord;
	int currDoc;
} u_w;


// Final Struct Defined to Transfer Final Strings
typedef struct oo {
	char result[MAX_STRING_LENGTH];
} tficfresult;


// Global Variables
int numDocs;
int numDocsToProcess; // Number of documents to be Processed by that particular rank
int *indexesToProcess; // Indexes of document names to Process

// Will hold all TFICF objects for asigned documents
obj TFICF[MAX_WORDS_IN_CORPUS];
int TF_idx = 0;

// Will hold all unique words in the corpus and the number of documents with that word
u_w unique_words[MAX_WORDS_IN_CORPUS];

// Will hold the final strings that will be printed out
word_document_str strings[MAX_WORDS_IN_CORPUS];
u_w* unique_words_global;

// MPI Custom Data Types defined for transferring structs among ranks
MPI_Datatype MPI_unique_word_type;
MPI_Datatype MPI_Result;

// Function for Equal Distribution of Work, called at every other rank except Rank 0.
void scheduleIndexes(int rank, int numproc);

// Function Declaration to get unique words at the Root node, to compute ICF Scores for every unique word
void getUniqueWordsRoot(int rank, int numproc);

// Function used at Root Node to find out ICF Values
void findICFStats(int rank, int numproc);

// Function to accumulate results back at Root Zero and print them back to a file.
void accumulateResults(int rank, int numproc);

// Comparison function between Strings used for qsort()
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


	/* -----------------------------------------------Creating a Custom MPI Datatype ------------------------------------- */
	int block_lengths[3] = {32,1,1};
	MPI_Datatype types[3] = {MPI_CHAR, MPI_INT, MPI_INT};
	MPI_Aint  offsets[3];

  offsets[0] = offsetof(u_w, word);
  offsets[1] = offsetof(u_w, numDocsWithWord);
	offsets[2] = offsetof(u_w, currDoc);

	MPI_Type_create_struct(3, block_lengths, offsets, types, &MPI_unique_word_type);
	MPI_Type_commit(&MPI_unique_word_type);
	/* -----------------------------------------------Creating a Custom MPI Datatype ------------------------------------- */


	// Important Variables in use
	DIR* files;
	struct dirent* file;
	int i,j;
	int docSize, contains;
	char filename[MAX_FILEPATH_LENGTH], word[MAX_WORD_LENGTH], document[MAX_DOCUMENT_NAME_LENGTH];

	int uw_idx = 0;
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
				numDocs++; // Finding numDocs.
			}
	}

	// BroadCast numDocs Inforation to all the Nodes.
	MPI_Bcast(&numDocs, 1, MPI_INT, ROOT, MPI_COMM_WORLD);

	if (rank != ROOT){

		  // Distributes Work across nodes and indexesToProcess array to store the indexes of the document to process.
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

	// unique_words_global matrix of size numproc*MAX_WORDS_IN_CORPUS
	unique_words_global = (u_w*)malloc(sizeof(u_w) * numproc * MAX_WORDS_IN_CORPUS);
	memset(unique_words_global, 0, sizeof(u_w) * numproc * MAX_WORDS_IN_CORPUS);

	// Accumulate results at Root Node
	getUniqueWordsRoot(rank, numproc);

	// Find out ICF stats at Root Node
	findICFStats(rank, numproc);

	MPI_Barrier(MPI_COMM_WORLD);

	// Send the Global ICF Array Information to all the nodes.
	MPI_Bcast(&unique_words, MAX_WORDS_IN_CORPUS, MPI_unique_word_type, ROOT, MPI_COMM_WORLD);

	// Finding TFICF information at all the local nodes
	if (rank != ROOT){
		for(i=0; i<TF_idx; i++) {
			for(j=0; j<MAX_WORDS_IN_CORPUS; j++) {
				if (!strcmp(TFICF[i].word, "") || !strcmp(unique_words[j].word, "")) continue;
				if(!strcmp(TFICF[i].word, unique_words[j].word)) {
					TFICF[i].numDocsWithWord = unique_words[j].numDocsWithWord;
					break;
				}
			}
		}

		// Print ICF job similar to HW4/HW5 (For debugging purposes)
		printf("------------ICF Job: Rank %d---------\n", rank);
		for(j=0; j<TF_idx; j++)
			printf("%s@%s\t%d/%d\n", TFICF[j].word, TFICF[j].document, TFICF[j].numDocs, TFICF[j].numDocsWithWord);
  }

	// Calculates TFICF value and puts: "document@word\tTFICF" into strings array
	for(j=0; j<TF_idx; j++) {
		double TF = log( 1.0 * TFICF[j].wordCount / TFICF[j].docSize + 1 );
		double ICF = log(1.0 * (TFICF[j].numDocs + 1) / (TFICF[j].numDocsWithWord + 1) );
		double TFICF_value = TF * ICF;

		// Copy the Computed Strings into the string array.
		sprintf(strings[j], "%s@%s\t%.16f", TFICF[j].document, TFICF[j].word, TFICF_value);
	}

	// Accumulate the Results at Root Zero.
	accumulateResults(rank, numproc);

	// Writes the text back to the file system.
	if (rank == ROOT){
				// Sort strings and print to file
			qsort(strings, TF_idx, sizeof(char)*MAX_STRING_LENGTH, myCompare);
			FILE* fp = fopen("output.txt", "w");
			if(fp == NULL){
				printf("Error Opening File: output.txt\n");
				exit(0);
			}
			for(i=0; i<TF_idx; i++)
				fprintf(fp, "%s\n", strings[i]);
			fclose(fp);
  }

	// Free the allocated servers
	if (rank == ROOT){
		// for (int i=0; i<numproc; ++i) {
		free(unique_words_global);
	}
	else{
		free(indexesToProcess);
	}

	// Close the MPI Communication.
	MPI_Finalize();
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

void getUniqueWordsRoot(int rank, int numproc){

    //Gathering manually

    if(rank == ROOT){

			MPI_Status stSendX;
			MPI_Request sendRqstX;

			MPI_Request rcvRqstX[numproc];
			MPI_Status rcvStX[numproc];

			MPI_Isend(unique_words, MAX_WORDS_IN_CORPUS, MPI_unique_word_type, ROOT, 0, MPI_COMM_WORLD, &sendRqstX);

      // Open Gates to Get the Results
      int rk;
      for (rk = 0; rk < numproc; rk++){
          MPI_Irecv(unique_words_global+rk*MAX_WORDS_IN_CORPUS, MAX_WORDS_IN_CORPUS, MPI_unique_word_type, rk, 0, MPI_COMM_WORLD, &rcvRqstX[rk]); // Tag 1 for tempY's
      }

      //Making wait calls to get all the message and to resend all the messages
      MPI_Waitall(numproc, rcvRqstX, rcvStX);
			MPI_Wait(&sendRqstX, &stSendX);

    }
    else {
				MPI_Status stSendX;
				MPI_Request sendRqstX;
        //if the rank is not zero send unique_words array to root 0
				MPI_Isend(unique_words, MAX_WORDS_IN_CORPUS, MPI_unique_word_type, ROOT, 0, MPI_COMM_WORLD, &sendRqstX);
        MPI_Wait(&sendRqstX, &stSendX);
    }
}

static int myCompareWords (const void *a, const void *b)
{
	  // Comparison function for sorting
		u_w* aword = (u_w *)a;
		u_w* bword = (u_w *)b;

    return -1*strcmp(aword->word, bword->word);
}

void findICFStats(int rank, int numproc){
	  if (rank != ROOT) return;

		// qsort() to sort the indexes.
		qsort(unique_words_global, numproc*MAX_WORDS_IN_CORPUS, sizeof(u_w), myCompareWords);


		int limit = numproc*MAX_WORDS_IN_CORPUS;
		int ind;

		// unique_words[uw_idx].numDocsWithWord;
		int uw_idx = 0;
		strcpy(unique_words[uw_idx].word, unique_words_global[0].word);
		unique_words[uw_idx].numDocsWithWord += unique_words_global[0].numDocsWithWord;
		for (ind = 1; ind < limit; ind++){
				if (strcmp(unique_words_global[ind].word, "") == 0) break;
				if (strcmp(unique_words_global[ind].word, unique_words[uw_idx].word) == 0){
						unique_words[uw_idx].numDocsWithWord += unique_words_global[ind].numDocsWithWord;
				}
				else{
					uw_idx++;
					strcpy(unique_words[uw_idx].word, unique_words_global[ind].word);
					unique_words[uw_idx].numDocsWithWord += unique_words_global[ind].numDocsWithWord;
				}
		}

		// Find the ICF stats and store them in unique_words array.
}

void accumulateResults(int rank, int numproc){

	// Create pointers to dynamically allocated arrays.
	tficfresult* localresults;
	int *numRecords;
	int *cummulativeValues;
	tficfresult* finalResults;
	int totalResults = 0;

	// Allocate Memory to pointers
	if (rank == ROOT) {
			numRecords = (int *)malloc(sizeof(int)*numproc);
			cummulativeValues = (int *)calloc(numproc, sizeof(int));
			TF_idx = 0;
 	}

	localresults = (tficfresult *)malloc(sizeof(tficfresult)*TF_idx);

	// Copy results into localresults array.
	int j;
	for(j=0; j<TF_idx; j++){
		 strcpy(localresults[j].result, strings[j]);
	}

	/* -----------------------------------------------Creating a Custom MPI Datatype ------------------------------------- */
	MPI_Gather(&TF_idx, 1, MPI_INT, numRecords, 1, MPI_INT, ROOT, MPI_COMM_WORLD);

	int block_lengths[1] = {MAX_STRING_LENGTH};
	MPI_Datatype types[1] = {MPI_CHAR};
	MPI_Aint  offsets[1] = {0};

	MPI_Type_create_struct(1, block_lengths, offsets, types, &MPI_Result);
	MPI_Type_commit(&MPI_Result);
	/* -----------------------------------------------Creating a Custom MPI Datatype ------------------------------------- */

	// MPI_Gatherv style communication to gather results from different nodes and different sized vectors
	if(rank == ROOT){
		int ind;

		for (ind = 1; ind<numproc; ind++){
				totalResults += numRecords[ind];
				cummulativeValues[ind] = cummulativeValues[ind-1] + numRecords[ind-1];
				// printf("%d, %d, %d, %d \n", ind, totalResults, numRecords[ind], cummulativeValues[ind]);
		}

		finalResults = (tficfresult *)malloc(sizeof(tficfresult)*totalResults);

		MPI_Request rcvRqstX[numproc-1];
		MPI_Status rcvStX[numproc-1];

		// printf("passed away   \n");

		// Open Gates to Get the Results
		int rk;
		for (rk = 1; rk < numproc; rk++){
				MPI_Irecv(finalResults+cummulativeValues[rk], numRecords[rk], MPI_Result, rk, 0, MPI_COMM_WORLD, &rcvRqstX[rk-1]); // Tag 1 for tempY's
		}
		//Making wait calls to get all the message and to resend all the messages
		MPI_Waitall(numproc-1, rcvRqstX, rcvStX);

	}
	else {
			MPI_Status stSendX;
			MPI_Request sendRqstX;
			//if the rank is not zero send unique_words array to root 0
			MPI_Isend(localresults, TF_idx, MPI_Result, ROOT, 0, MPI_COMM_WORLD, &sendRqstX);
			MPI_Wait(&sendRqstX, &stSendX);
	}

	// Free Resources
	if (rank == ROOT){
			int ind;
			for (ind=0; ind<totalResults; ind++){
					strcpy(strings[ind], finalResults[ind].result);
			}
		free(finalResults);
		free(numRecords);
		free(cummulativeValues);
	}

	free(localresults);
	
	TF_idx = totalResults;
}
