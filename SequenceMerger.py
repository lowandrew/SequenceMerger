from RedmineAPI.Utilities import FileExtension, create_time_log
from RedmineAPI.Access import RedmineAccess
from RedmineAPI.Configuration import Setup
import pandas as pd
from pandas import ExcelWriter
import os
import shutil
import glob

from Utilities import CustomKeys, CustomValues


def convert_excel_file(infile, outfile):
    df = pd.read_excel(infile)
    to_keep = ['SEQID', 'OtherName']
    for column in df:
        if column not in to_keep:
            df = df.drop(column, axis=1)
    df = df.rename(columns={'SEQID': 'Name', 'OtherName': 'Merge'})
    writer = ExcelWriter(outfile)
    df.to_excel(writer, 'Sheet1', index=False)
    writer.save()


def generate_seqid_list(mergefile):
    df = pd.read_excel(mergefile)
    seqid_list = list()
    seqids = list(df['Merge'])
    for row in seqids:
        for item in row.split(';'):
            seqid_list.append(item)
    return seqid_list


class Automate(object):

    def __init__(self, force):

        # create a log, can be written to as the process continues
        self.timelog = create_time_log(FileExtension.runner_log)

        # Key: used to index the value to the config file for setup
        # Value: 3 Item Tuple ("default value", ask user" - i.e. True/False, "type of value" - i.e. str, int....)
        # A value of None is the default for all parts except for "Ask" which is True
        # custom_terms = {CustomKeys.key_name: (CustomValues.value_name, True, str)}  # *** can be more than 1 ***
        custom_terms = dict()

        # Create a RedmineAPI setup object to create/read/write to the config file and get default arguments
        setup = Setup(time_log=self.timelog, custom_terms=custom_terms)
        setup.set_api_key(force)

        # Custom terms saved to the config after getting user input
        # self.custom_values = setup.get_custom_term_values()
        # *** can be multiple custom values variable, just use the key from above to reference the inputted value ***
        # self.your_custom_value_name = self.custom_values[CustomKeys.key_name]

        # Default terms saved to the config after getting user input
        self.seconds_between_checks = setup.seconds_between_check
        self.nas_mnt = setup.nas_mnt
        self.redmine_api_key = setup.api_key

        # Initialize Redmine wrapper
        self.access_redmine = RedmineAccess(self.timelog, self.redmine_api_key)

        self.botmsg = '\n\n_I am a bot. This action was performed automatically._'  # sets bot message
        # Subject name and Status to be searched on Redmine
        self.issue_title = 'merge'  # must be a lower case string to validate properly
        self.issue_status = 'New'

    def timed_retrieve(self):
        """
        Continuously search Redmine in intervals for the inputted period of time, 
        Log errors to the log file as they occur
        """
        import time
        while True:
            # Get issues matching the issue status and subject
            found_issues = self.access_redmine.retrieve_issues(self.issue_status, self.issue_title)
            # Respond to the issues in the list 1 at a time
            while len(found_issues) > 0:
                self.respond_to_issue(found_issues.pop(len(found_issues) - 1))
            self.timelog.time_print("Waiting for the next check.")
            time.sleep(self.seconds_between_checks)

    def respond_to_issue(self, issue):
        """
        Run the desired automation process on the inputted issue, if there is an error update the author
        :param issue: Specified Redmine issue information
        """
        self.timelog.time_print("Found a request to run. Subject: %s. ID: %s" % (issue.subject, str(issue.id)))
        self.timelog.time_print("Adding to the list of responded to requests.")
        self.access_redmine.log_new_issue(issue)

        try:
            issue.redmine_msg = "Beginning the process for: %s" % issue.subject
            self.access_redmine.update_status_inprogress(issue, self.botmsg)
            ##########################################################################################
            # Make the bio_request folder.
            workdir = '/mnt/nas/bio_requests/' + str(issue.id)
            os.makedirs(workdir)
            current_dir = os.getcwd()
            # Step 1: Download the excel file from Redmine.
            try:
                excel_file = self.access_redmine.get_attached_files(issue)
                address = excel_file[0]['content_url']
                file_contents = self.access_redmine.redmine_api.download_file(address, decode=False)
                f = open(workdir + '/Merge_Request.xlsx', 'wb')
                f.write(file_contents)
                f.close()
            except IndexError:
                self.access_redmine.update_issue_to_author(issue, message="You did not upload any files. Please create a new"
                                                                           " issue, upload a Merge Request excel file, and "
                                                                            "try again.")
            # Step 2: Create file readable by Adam's Merger script (done in convert_excel_file)
            convert_excel_file(workdir + '/Merge_Request.xlsx', workdir + '/Merge.xlsx')
            # Step 3: Make seqid list for file extraction (done by generate_seqid_list on outfile from convert_excel_file)
            seqid_list = generate_seqid_list(workdir + '/Merge.xlsx')
            # Write the seqID list so that things can be extracted.
            f = open(workdir + '/seqidlist.txt', 'w')
            for seqid in seqid_list:
                f.write(seqid + '\n')
            f.close()
            # Step 4: Extract fastq files from MiSeq Backup w/ file_linker.py into the bio_request folder.
            os.chdir('/mnt/nas/MiSeq_Backup')
            cmd = 'python2 file_linker.py {} {}'.format(workdir + '/seqidlist.txt', workdir)
            os.system(cmd)
            # Step 5: Run merger.py script on those files.
            os.chdir(current_dir)
            cmd = 'python merger.py -f {} -d ";" {}'.format(workdir + '/Merge.xlsx', workdir)
            print(cmd)
            os.system(cmd)
            # Step 6: Make a folder of those files and put it into To_Assemble/Copy to merge_Backup
            # Make a folder within the bio_request folder.
            os.makedirs(workdir + '/merge_' + str(issue.id))
            merged_fastqs = glob.glob(workdir + '/*MER*/*.fastq.gz')
            for fastq in merged_fastqs:
                shutil.move(fastq, workdir + '/merge_' + str(issue.id))
            # Copy the merged files to merge_Backup
            merged_fastqs = glob.glob(workdir + '/merge_' + str(issue.id) + '/*.fastq.gz')
            if len(merged_fastqs) == 0:
                raise FileNotFoundError('Could not find any merged fastq files. The merge script likely failed. '
                                        'Please ensure that your excel file is properly formatted.')
            print('Copying files to merge_Backup...')
            for fastq in merged_fastqs:
                shutil.copy(fastq, '/mnt/nas/merge_Backup')
            # Copy the files to To_Assemble and delete from the bio_request folder/
            merged_fastqs = glob.glob(workdir + '/merge_' + str(issue.id) + '/*.fastq.gz')
            os.makedirs('/mnt/nas/To_Assemble/merge_' + str(issue.id))
            print('Moving files to To_Assemble...')
            for fastq in merged_fastqs:
                shutil.move(fastq, '/mnt/nas/To_Assemble/merge_' + str(issue.id))
            os.rename('/mnt/nas/To_Assemble/merge_' + str(issue.id), '/mnt/nas/To_Assemble/merge_' + str(issue.id) + '_Ready')
            ##########################################################################################
            self.completed_response(issue)

        except Exception as e:
            import traceback
            self.timelog.time_print("[Warning] The automation process had a problem, continuing redmine api anyways.")
            self.timelog.time_print("[Automation Error Dump]\n" + traceback.format_exc())
            # Send response
            issue.redmine_msg = "There was a problem with your request. Please create a new issue on" \
                                " Redmine to re-run it.\n%s" % traceback.format_exc()
            # Set it to feedback and assign it back to the author
            self.access_redmine.update_issue_to_author(issue, self.botmsg)

    def completed_response(self, issue):
        """
        Update the issue back to the author once the process has finished
        :param issue: Specified Redmine issue the process has been completed on
        """
        # Assign the issue back to the Author
        self.timelog.time_print("Assigning the issue: %s back to the author." % str(issue.id))

        issue.redmine_msg = "Merged FastQs have been place in merge_Backup and To_Assemble. Process complete!"
        # Update author on Redmine
        self.access_redmine.update_issue_to_author(issue, self.botmsg)

        # Log the completion of the issue including the message sent to the author
        self.timelog.time_print("\nMessage to author - %s\n" % issue.redmine_msg)
        self.timelog.time_print("Completed Response to issue %s." % str(issue.id))
        self.timelog.time_print("The next request will be processed once available")
