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



Tips:

-   Use `ipython3`instead of `python`
