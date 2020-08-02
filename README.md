Demo: https://supplementary.herokuapp.com/

Analyze and visualize results of all DTU students.

Project setup:

-   Download **python3.7.6**

-   Create a virtual environment in the root directory

    ```shell
    python3 -m venv venv
    ```

-   Acivate the virtual env:

    ```shell
    source venv/bin/activate
    ```

    **Optional**: We can use auto env to automate this step and export of other env vars with [autoenv](https://github.com/inishchith/autoenv). This program allows us to set commands that will run every time we cd into our directory. In order to use it, we will need to install it globally. First, exit out of your virtual environment in the terminal, install autoenv, then and add a *.env* file:

    ```shell
    $ deactivate
    $ pip install autoenv==1.0.0
    $ touch .env
    ```

-   Install required packages:

    ```shell
    $ pip install -r requirements.txt
    ```

-   Sync /data from dropbox

    This step downloads

    -   Raw pdf results files
    -   Caches to speedup parsing of pdfs during development
    -   Update the parsed data

    Steps:

    1.  Export `SUPPLEMENTARY_DROPBOX_TOKEN=<token>`in your environment.

    2.  From the root dir of your clone, run the following command.

        ```python src/python/dropbox_updown.py data data```

    This will clone all the data from the dropbox storage. The above command needs to be run everytime after you make changes to the `/data`folder. Please note that we don't track the contents of the folder using git.

#### Dropbox Usage

We store the raw and parsed data and other shareable things like caches to a dropbox space. You will need to ask for `SUPPLEMENTARY_DROPBOX_TOKEN`from the maintainers to get started.

```python
>>> import dropbox
>>> dbx = dropbox.Dropbox('<token>');
```

List files/folders in a directory.

```python
>>> [entry.name for entry in dbx.files_list_folder('').entries]
['pdf', 'data']

>>> [entry.name for entry in dbx.files_list_folder('/data').entries]
['dtu_results', 'parsed_data.json']

>>> len([entry.name for entry in dbx.files_list_folder('/data/dtu_results').entries])
1319
```

Tips:

-   Use `ipython3`instead of `python`
