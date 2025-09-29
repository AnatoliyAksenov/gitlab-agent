import re

import gitlab
from gitlab.exceptions import GitlabCreateError


class GitWorker():
    def __init__(self, gitlab_url:str, gitlab_token:str, project_path:str):
        self.__gitlab_url = gitlab_url
        self.__gitlab_token = gitlab_token
        self.gl = gitlab.Gitlab(self.__gitlab_url, private_token=self.__gitlab_token)
        self.project = self.gl.projects.get(project_path)
        self.project_id = self.project.id



    def jira_create_branch(self, issue_id:int, prefix:str="feature/LCT-", postfix="-de-agent") -> str:

        new_branch_name = f'{prefix}{str(issue_id).rjust(3, '0')}{ str(self.project_id).rjust(3, '0') }{postfix}'
        source_ref = 'main' # Create from the 'main' branch

        try:
            branch = self.project.branches.create({'branch': new_branch_name, 'ref': source_ref})
            
            return branch.name
        except GitlabCreateError as e:
    
            try:
                # if branch already exists try to get branch by name
                branch = self.project.branches.get(new_branch_name)
                return branch.name
            except:
                raise e
            
    def gitlab_branch_name(self, issue_id:int, task_title:str) -> str:

        prepared_task_title = re.sub(r'\W+','-',task_title.lower())
        branch_name = f'{str(issue_id)}-{prepared_task_title}'

        return branch_name

    def gitlab_create_branch(self, issue_id:int, task_title:str) -> str:

        new_branch_name = self.gitlab_branch_name(issue_id, task_title)
        source_ref = 'main' # Create from the 'main' branch

        try:
            branch = self.project.branches.create({'branch': new_branch_name, 'ref': source_ref})
            
            return branch.name
        except GitlabCreateError as e:
    
            try:
                # if branch already exists try to get branch by name
                branch = self.project.branches.get(new_branch_name)
                return branch.name
            except:
                raise e

        
    def gitlab_commit_file(self, branch_name:str, filename:str, filecontent:str, task:str):
    
            actions = [
                {
                    'action': 'create',
                    'file_path': f'{filename}',
                    'content': filecontent
                }
            ]
        
            try:
                commit = self.project.commits.create({
                    'branch': branch_name, 
                    'commit_message': task,
                    'actions': actions
                })
                return {"task": task, "success": True, "commit": commit}
            except Exception as e:
                return {"task": task, "success": False}
            
    def add_notes(self, issue_id, message):
        issue = self.project.issues.get(issue_id)
        issue.notes.create({"body": message})


    def close_issue(self, issue_id):
        issue = self.project.issues.get(issue_id)
        issue.state_event = 'close'
        issue.save()


    def create_merge_request(self, issue_id) -> bool:

        issue = self.project.issues.get(issue_id)
        title = issue.title

        branch_name = self.gitlab_branch_name(issue_id, title)
        branch = self.project.branches.get(branch_name)
        if branch:
            data = {
                'source_branch': branch_name,
                'target_branch': 'main',
                'title': title,
                'labels': issue.labels
            }
            self.project.mergerequests.create(data)

            return True
        
        return False
    