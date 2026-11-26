"""
The stores: what a component asks of the database, behind one boundary.

Nothing is re-exported here on purpose. The drivers import ``schema`` from this
package, and a re-export would load modules that import the drivers.
"""
