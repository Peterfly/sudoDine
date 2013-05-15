grep 'type": "review' yelp_data.json | head -n 200 > temp.json
grep 'type": "business' yelp_data.json | head -n 200 >> temp.json
grep 'type": "user' yelp_data.json | head -n 200 >> temp.json