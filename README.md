This repository contains the code for fixing Maharashtra village boundaries which is yet to be pushed on dolr repository.

Following tasks are carried out:

  1. primal.py
    1) Create village midlines (code already present in dolr repository)
    2) Add void polygon by taking the complement of the union of villages
    3) Find triple points and label them by the villages to which it connects
    4) Create a topological graph of the villages using these triple points
    5) Project these triple points on the original village boundaries, split the original village boundaries using these projected points
    6) Split the village midlines using the triple points and label the splitted midlines by the names of two villages that are adjacent to them

  2. vb_label.py
    1) If there is a stream in both villages, then we label that part of midline as ‘r-r’
    2) If there is a stream in one village while not in another, then we label that part of midline as ‘r-s’
    3) Else we label the part of midline as ‘s-s’

  3. fix_vb.py (work in progress)
    Handles the cases of 'r-r', 'r-s' and 's-s'
