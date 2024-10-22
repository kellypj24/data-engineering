1. setup postgres
2. setup secrets & roles 
3. run bootstrap image as one-shot / standalone task
    - fill in `<blanks>` with info from `1` & `2`
4. run temporal cluster as service
    - fill in `<blanks>` with info from `1` & `2`
    - may need to break each container into its own task for independent scaling later